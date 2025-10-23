import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

type Existence = {
  databaseExists?: boolean;
  serviceUserExists?: boolean;
  cdcUserExists?: boolean;
};

describe('bootstrap.handler – unit', () => {
  const secretsMock = mockClient(SecretsManagerClient);

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  /**
   * Fully controlled testbed. Allows:
   *  - precise env overrides
   *  - database / role existence flags
   *  - CDC secret shape (undefined | null SecretString | partial | full)
   *  - capturing every pg.Client constructor arg for every connection
   */
  function setup(
    envOverrides: Partial<{
      MASTER_USER_SECRET: string;
      APP_USER_SECRET: string;
      CDC_USER_SECRET: string | undefined;
      APP_DATABASE_NAME: string | undefined;
      APP_SCHEMA_NAME: string | undefined;
      RDS_HOST: string;
    }> = {},
    existence: Existence = {},
    cdcSecret:
      | { username?: string; password?: string }
      | null
      | undefined = undefined,
  ) {
    const env = {
      MASTER_USER_SECRET: 'master-test',
      APP_USER_SECRET: 'app-test',
      CDC_USER_SECRET: undefined as string | undefined,
      APP_DATABASE_NAME: 'app_database',
      APP_SCHEMA_NAME: 'app_schema',
      RDS_HOST: 'example',
      ...envOverrides,
    };

    const {
      databaseExists = false,
      serviceUserExists = false,
      cdcUserExists = false,
    } = existence;

    // Resolve secrets
    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.MASTER_USER_SECRET! })
      .resolves({
        SecretString: JSON.stringify({
          username: 'admin_user',
          password: 'admin_password',
        }),
      });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.APP_USER_SECRET! })
      .resolves({
        SecretString: JSON.stringify({
          username: 'myapp_user',
          password: 'myapp_password',
        }),
      });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? ({} as any) // simulate SecretString missing
          : {
              SecretString:
                cdcSecret === undefined
                  ? JSON.stringify({
                      username: 'cdc_user',
                      password: 'cdc_password',
                    })
                  : JSON.stringify(cdcSecret),
            };
      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    // pg.Client stubs
    const mainClient = {
      database: 'postgres',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (sql.includes('pg_catalog.pg_database')) {
          return { rows: [{ exists: databaseExists }] };
        }
        if (sql.includes(`rolname='myapp_user'`)) {
          return { rows: [{ exists: serviceUserExists }] };
        }
        if (sql.includes(`rolname='cdc_user'`)) {
          return { rows: [{ exists: cdcUserExists }] };
        }
        return { rows: [{}] };
      }),
    };

    const serviceClient = {
      database: env.APP_DATABASE_NAME ?? 'myapp',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    const cdcClient = {
      database: env.APP_DATABASE_NAME ?? 'myapp',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    // capture constructor args to kill ObjectLiteral mutants for each Client
    const ctorArgs: any[] = [];
    let call = 0;
    const ClientStub = sinon.stub().callsFake((opts: any) => {
      ctorArgs.push(opts);
      call += 1;
      if (!opts || !('database' in opts)) return mainClient as any; // main
      if (env.CDC_USER_SECRET && call >= 3) return cdcClient as any; // cdc conn
      return serviceClient as any; // service
    });

    const mod = proxyquire('../../src/bootstrap', {
      pg: { Client: ClientStub },
      envalid: { cleanEnv: () => env },
    });

    return {
      handler: mod.handler as () => Promise<{ message: string }>,
      env,
      mainClient,
      serviceClient,
      cdcClient,
      ClientStub,
      ctorArgs,
    };
  }

  //
  // --- Primary happy path (no CDC), DB & user missing -> created
  //
  it('creates DB and service user when missing; grants & owner changes applied (no CDC)', async () => {
    const { handler, mainClient, serviceClient, ctorArgs } = setup();

    const out = await handler();
    expect(out).to.deep.equal({
      message: "Database 'app_database' usernames are ready for use!",
    });

    const mainSQL = mainClient.query.getCalls().map((c) => c.args[0] as string);
    expect(mainSQL).to.include("CREATE DATABASE app_database");
    expect(mainSQL).to.include(
      "CREATE USER myapp_user WITH ENCRYPTED PASSWORD 'myapp_password'",
    );

    const svcSQL = serviceClient.query
      .getCalls()
      .map((c) => c.args[0] as string);

    // A few high-signal grants covering many mutants on this block
    expect(svcSQL).to.include(
      'CREATE SCHEMA IF NOT EXISTS app_schema',
    );
    expect(svcSQL).to.include(
      'GRANT CONNECT ON DATABASE app_database TO myapp_user',
    );
    expect(svcSQL).to.include(
      'GRANT USAGE, CREATE ON SCHEMA app_schema TO myapp_user',
    );
    expect(svcSQL).to.include(
      "ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user",
    );
    expect(svcSQL).to.include(
      'ALTER DATABASE app_database OWNER TO myapp_user',
    );

    // Kill object literal mutants for main & service connections
    expect(ctorArgs[0]).to.deep.equal({
      user: 'admin_user',
      password: 'admin_password',
      host: 'example',
      port: 5432,
    });
    expect(ctorArgs[1]).to.deep.equal({
      database: 'app_database',
      user: 'admin_user',
      password: 'admin_password',
      host: 'example',
      port: 5432,
    });
  });

  //
  // --- No-op creation when DB/user already exist
  //
  it('skips CREATE DATABASE and CREATE USER when they already exist', async () => {
    const { handler, mainClient } = setup(
      {},
      { databaseExists: true, serviceUserExists: true },
    );
    await handler();

    const mainSQL = mainClient.query.getCalls().map((c) => c.args[0] as string);
    expect(mainSQL.some((s) => s.startsWith('CREATE DATABASE'))).to.equal(false);
    expect(mainSQL.some((s) => s.startsWith('CREATE USER'))).to.equal(false);
  });

  //
  // --- Database & schema fallbacks (kills string-literal & logical-operator mutants)
  //
  it('derives database from username.replace and schema from username when env values are undefined', async () => {
    const { handler, serviceClient, ctorArgs } = setup({
      APP_DATABASE_NAME: undefined, // => "myapp_user".replace('_user','') => "myapp"
      APP_SCHEMA_NAME: undefined, // => "myapp_user"
    });

    const out = await handler();
    expect(out.message).to.equal("Database 'myapp' usernames are ready for use!");

    const svcSQL = serviceClient.query
      .getCalls()
      .map((c) => c.args[0] as string);

    // If mutated to &&, schema becomes "undefined" and these asserts fail.
    expect(svcSQL).to.include('CREATE SCHEMA IF NOT EXISTS myapp_user');
    expect(svcSQL).to.include(
      'GRANT USAGE, CREATE ON SCHEMA myapp_user TO myapp_user',
    );

    // Also validate the derived database was used in the connection object
    expect(ctorArgs[1]).to.deep.equal({
      database: 'myapp',
      user: 'admin_user',
      password: 'admin_password',
      host: 'example',
      port: 5432,
    });
  });

  //
  // --- CDC: user missing -> create + grants + publication
  //
  it('creates CDC user when missing and applies grants/publication', async () => {
    const { handler, mainClient, cdcClient, ctorArgs } = setup(
      { CDC_USER_SECRET: 'cdc-secret' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    await handler();

    const mainSQL = mainClient.query.getCalls().map((c) => c.args[0] as string);
    expect(mainSQL).to.include(
      "SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')",
    );
    expect(mainSQL).to.include(
      "CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'cdc_password'",
    );

    const cdcSQL = cdcClient.query.getCalls().map((c) => c.args[0] as string);
    expect(cdcSQL).to.include(
      'GRANT CONNECT ON DATABASE app_database TO cdc_user',
    );
    expect(cdcSQL).to.include(
      'GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user',
    );
    expect(cdcSQL).to.include('GRANT rds_replication, rds_superuser TO cdc_user');
    expect(cdcSQL).to.include(
      'CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES',
    );

    // Kill object-literal mutant for CDC connection construction as well
    expect(ctorArgs[2]).to.deep.equal({
      database: 'app_database',
      user: 'admin_user',
      password: 'admin_password',
      host: 'example',
      port: 5432,
    });
  });

  //
  // --- CDC: user already exists -> no CREATE USER but still grants
  //
  it('does not create CDC user when it exists but still applies grants', async () => {
    const { handler, mainClient, cdcClient } = setup(
      { CDC_USER_SECRET: 'cdc-secret' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    await handler();

    const mainSQL = mainClient.query.getCalls().map((c) => c.args[0] as string);
    expect(mainSQL).to.include(
      "SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')",
    );
    expect(mainSQL.some((s) => s.startsWith('CREATE USER cdc_user'))).to.equal(
      false,
    );

    const cdcSQL = cdcClient.query.getCalls().map((c) => c.args[0] as string);
    expect(cdcSQL).to.include(
      'GRANT CONNECT ON DATABASE app_database TO cdc_user',
    );
    expect(cdcSQL).to.include(
      'GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user',
    );
  });

  //
  // --- CDC disabled or unusable secret -> CDC connection never created
  //
  it('skips CDC flow when CDC SecretString is null', async () => {
    const { handler, cdcClient } = setup(
      { CDC_USER_SECRET: 'cdc-secret' },
      {},
      null, // Secrets Manager returned no SecretString
    );
    await handler();
    expect(cdcClient.connect.called).to.equal(false);
    expect(cdcClient.query.called).to.equal(false);
  });

  it('skips CDC flow when CDC secret has no username or password', async () => {
    const { handler, cdcClient } = setup(
      { CDC_USER_SECRET: 'cdc-secret' },
      {},
      { password: undefined, username: undefined },
    );
    await handler();
    // We still open the CDC DB connection only when cdcUserSecret is defined;
    // in this case it is defined but unusable → no CDC grants run.
    expect(cdcClient.connect.called).to.equal(true);
    const cdcSQL = cdcClient.query.getCalls().map((c) => c.args[0] as string);
    expect(cdcSQL.some((q) => q.startsWith('GRANT CONNECT ON DATABASE'))).to.equal(false);
  });
});
