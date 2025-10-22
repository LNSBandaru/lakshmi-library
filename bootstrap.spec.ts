import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

type ExistenceOverrides = {
  databaseExists?: boolean;
  serviceUserExists?: boolean;
  cdcUserExists?: boolean;
};

describe('Handler - Unit (100% mutation)', () => {
  let secretsMock: ReturnType<typeof mockClient>;

  before(() => {
    secretsMock = mockClient(SecretsManagerClient);
  });

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  function setUp(
    envOverrides: Record<string, any> = {},
    existenceOverrides: ExistenceOverrides = {},
    cdcSecret: { username?: string; password?: string } | null | undefined = undefined
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
    } = existenceOverrides;

    const appDatabase = env.APP_DATABASE_NAME ?? 'myapp';
    const expectedSchema = env.APP_SCHEMA_NAME ?? 'myapp_user';

    // Secrets mocks
    secretsMock.on(GetSecretValueCommand, { SecretId: 'master-test' }).resolves({
      SecretString: JSON.stringify({ username: 'admin_user', password: 'admin_password' }),
    });
    secretsMock.on(GetSecretValueCommand, { SecretId: 'app-test' }).resolves({
      SecretString: JSON.stringify({ username: 'myapp_user', password: 'myapp_password' }),
    });
    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {} // SecretString undefined
          : { SecretString: JSON.stringify(cdcSecret ?? { username: 'cdc_user', password: 'cdc_password' }) };
      secretsMock.on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET }).resolves(payload as any);
    }

    // PG stubs — we need to check many strings -> keep query capture
    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (sql.includes('pg_catalog.pg_database')) {
          return { rows: [{ exists: databaseExists }] };
        }
        if (sql.includes(`FROM pg_roles`) && sql.includes(`'myapp_user'`)) {
          return { rows: [{ exists: serviceUserExists }] };
        }
        if (sql.includes(`FROM pg_roles`) && (sql.includes(`'cdc_user'`) || sql.includes(`'partial_user'`))) {
          return { rows: [{ exists: cdcUserExists }] };
        }
        return { rows: [{}] };
      }),
    };

    const serviceClientStub = {
      database: appDatabase,
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    const cdcClientStub = {
      database: appDatabase,
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    // We want deterministic order: 1) main, 2) service, 3) cdc (when present)
    let ctor = 0;
    const pgStub = sinon.stub().callsFake((opts: any) => {
      ctor++;
      if (!opts.database) return mainClientStub;
      if (opts.database === appDatabase && ctor === 2) return serviceClientStub;
      if (opts.database === appDatabase && ctor === 3 && env.CDC_USER_SECRET) return cdcClientStub;
      return serviceClientStub;
    });

    const handler = proxyquire('../../src/bootstrap', {
      pg: { Client: pgStub },
      envalid: { cleanEnv: () => env },
    });

    return {
      handler,
      env,
      appDatabase,
      expectedSchema,
      pgStub,
      mainClientStub,
      serviceClientStub,
      cdcClientStub,
    };
  }

  const expectedMsg = "Database 'app_database' usernames are ready for use!";

  // ============ Base/Non-CDC ============

  it('completes base path (no CDC) and uses exact main client options', async () => {
    const { handler, pgStub } = setUp();

    const result = await handler.handler();
    expect(result).to.deep.equal({ message: expectedMsg });

    // Kill ObjectLiteral mutant around main Client options
    const firstCallArgs = pgStub.getCall(0).args[0];
    expect(firstCallArgs).to.deep.equal({
      user: 'admin_user',
      password: 'admin_password',
      host: 'example',
      port: 5432,
    });
  });

  it('uses default schema when APP_SCHEMA_NAME is undefined (kills LogicalOperator mutant)', async () => {
    const { handler, serviceClientStub, expectedSchema } = setUp({ APP_SCHEMA_NAME: undefined });
    await handler.handler();
    const S = (s: string) => serviceClientStub.query.getCalls().map((c) => c.args[0]).some((x) => x.includes(s));
    expect(S(`CREATE SCHEMA IF NOT EXISTS ${expectedSchema}`)).to.equal(true);
    expect(S(`CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA ${expectedSchema}`)).to.equal(true);
    expect(S(`CREATE EXTENSION IF NOT EXISTS intarray SCHEMA ${expectedSchema}`)).to.equal(true);
    expect(S(`GRANT USAGE, CREATE ON SCHEMA ${expectedSchema}`)).to.equal(true);
    expect(S(`ALTER DEFAULT PRIVILEGES IN SCHEMA ${expectedSchema}`)).to.equal(true);
  });

  it('executes full service grant block (kills StringLiteral/BlockStatement survivors)', async () => {
    const { handler, serviceClientStub, appDatabase, expectedSchema } = setUp();
    await handler.handler();

    const calls = serviceClientStub.query.getCalls().map((c) => c.args[0]);

    // Every grant / revoke / extension statement must be hit
    expect(calls).to.deep.include(`CREATE SCHEMA IF NOT EXISTS ${expectedSchema}`);
    expect(calls).to.deep.include(`CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA ${expectedSchema} CASCADE`);
    expect(calls).to.deep.include(`CREATE EXTENSION IF NOT EXISTS intarray SCHEMA ${expectedSchema} CASCADE`);
    expect(calls).to.deep.include(`GRANT CONNECT ON DATABASE ${appDatabase} TO myapp_user`);
    expect(calls).to.deep.include(`GRANT CREATE ON DATABASE ${appDatabase} TO myapp_user`);
    expect(calls).to.deep.include(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`);
    expect(calls).to.deep.include(`REVOKE ALL ON DATABASE ${appDatabase} FROM PUBLIC`);
    expect(calls).to.deep.include(`GRANT USAGE, CREATE ON SCHEMA ${expectedSchema} TO myapp_user`);
    expect(calls).to.deep.include(
      `ALTER DEFAULT PRIVILEGES IN SCHEMA ${expectedSchema} GRANT ALL PRIVILEGES ON TABLES TO myapp_user`
    );
    expect(calls).to.deep.include(`GRANT ALL PRIVILEGES on DATABASE ${appDatabase} to myapp_user`);
    expect(calls).to.deep.include(`ALTER DATABASE ${appDatabase} OWNER TO myapp_user`);
  });

  it('skips DB and user creation when both already exist', async () => {
    const { handler, mainClientStub } = setUp({}, { databaseExists: true, serviceUserExists: true });
    await handler.handler();
    const mainSQL = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(mainSQL.filter((s) => s.includes('CREATE')).length).to.equal(0);
  });

  it('skips CDC entirely when env.CDC_USER_SECRET is undefined', async () => {
    const { handler, cdcClientStub } = setUp({}, {}, undefined);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.equal(false);
    expect(result.message).to.equal(expectedMsg);
  });

  // ============ CDC paths ============

  it('CDC: creates user and grants when user missing', async () => {
    const { handler, mainClientStub, cdcClientStub, appDatabase, expectedSchema } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_password' }
    );
    const result = await handler.handler();
    const mainSQL = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(mainSQL).to.include(`CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'cdc_password'`);

    const cdcSQL = cdcClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(cdcSQL).to.include(`GRANT CONNECT ON DATABASE ${appDatabase} TO cdc_user`);
    expect(cdcSQL).to.include(`GRANT SELECT ON ALL TABLES IN SCHEMA ${expectedSchema} TO cdc_user`);
    expect(cdcSQL).to.include(`GRANT rds_replication, rds_superuser TO cdc_user`);
    expect(cdcSQL).to.include(`CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES`);

    expect(result.message).to.equal(expectedMsg);
  });

  it('CDC: skips user creation when already exists', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' }
    );
    const result = await handler.handler();
    const allSQL = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(allSQL).to.include(`SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`);
    expect(allSQL).to.not.include(`CREATE USER cdc_user`);
    expect(result.message).to.equal(expectedMsg);
  });

  it('CDC: executes block even when username missing (truthy cdcUserSecret)', async () => {
    const { handler, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { password: 'only-pass' } // username missing
    );
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.equal(true); // branch hit
    const grants = cdcClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(grants).to.include(`GRANT CONNECT ON DATABASE`);
    expect(result.message).to.equal(expectedMsg);
  });

  it('CDC: skips entire flow when SecretString is null', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, null);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.equal(false);
    expect(result.message).to.equal(expectedMsg);
  });

  it('CDC: supports partial username only', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { username: 'partial_user' }
    );
    const result = await handler.handler();
    const mainSQL = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(mainSQL).to.include(`rolname='partial_user'`);
    expect(result.message).to.equal(expectedMsg);
  });

  it('CDC: returns message even when everything pre-exists', async () => {
    const { handler } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { databaseExists: true, serviceUserExists: true, cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' }
    );
    const result = await handler.handler();
    expect(result).to.deep.equal({ message: expectedMsg });
  });
});
