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


// ************** STEP-1 *******
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

/**
 * This spec exercises every branch in src/bootstrap.ts and asserts on the
 * exact SQL fragments so that string literal and block-statement mutants die.
 *
 * Scenarios covered:
 *  - DB exists / not exists (CREATE DATABASE guarded)
 *  - Service user exists / not exists (CREATE USER guarded)
 *  - CDC disabled (no secret id)
 *  - CDC declared but SecretString is null/undefined (skip whole CDC flow)
 *  - CDC enabled (user exists / not exists) + all CDC GRANTs & PUBLICATION
 *  - Defaults when APP_DATABASE_NAME / APP_SCHEMA_NAME are undefined
 *  - Return message includes computed database
 *  - The query() helper logs with [database] prefix
 */

describe('Handler - Unit', () => {
  let secretsMock: ReturnType<typeof mockClient>;
  before(() => {
    secretsMock = mockClient(SecretsManagerClient);
  });

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  type Env = {
    MASTER_USER_SECRET: string;
    APP_USER_SECRET: string;
    CDC_USER_SECRET?: string;
    APP_DATABASE_NAME?: string;
    APP_SCHEMA_NAME?: string;
    RDS_HOST: string;
  };

  type ExistsFlags = {
    databaseExists?: boolean;
    serviceUserExists?: boolean;
    cdcUserExists?: boolean;
  };

  /**
   * Test harness
   */
  function setUp(
    envOverrides: Partial<Env> = {},
    existenceOverrides: ExistsFlags = {},
    // cdcSecret:
    //   - undefined => no CDC_USER_SECRET in env
    //   - null      => CDC_USER_SECRET set, but AWS returns { SecretString: undefined }
    //   - object    => CDC secrets payload (truthy => CDC flow executed)
    cdcSecret: any = undefined,
  ) {
    // Fixed secrets used by the code under test
    const MASTER_SECRET_ID = 'master-test';
    const APP_SECRET_ID = 'app-test';

    // The app user in APP secret drives the "default" db/schema derivation
    const serviceSecret = { username: 'myapp_user', password: 'myapp_password' };
    const masterSecret = { username: 'admin_user', password: 'admin_password' };

    const baseEnv: Env = {
      MASTER_USER_SECRET: MASTER_SECRET_ID,
      APP_USER_SECRET: APP_SECRET_ID,
      CDC_USER_SECRET: undefined,
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

    // What database/schema the code will end up using
    const derivedDatabase =
      baseEnv.APP_DATABASE_NAME ?? serviceSecret.username.replace('_user', '');
    const derivedSchema =
      baseEnv.APP_SCHEMA_NAME ?? serviceSecret.username;

    // --- Stub AWS Secrets Manager
    secretsMock
      .on(GetSecretValueCommand, { SecretId: MASTER_SECRET_ID })
      .resolves({ SecretString: JSON.stringify(masterSecret) });
    secretsMock
      .on(GetSecretValueCommand, { SecretId: APP_SECRET_ID })
      .resolves({ SecretString: JSON.stringify(serviceSecret) });

    if (baseEnv.CDC_USER_SECRET) {
      // Simulate return with/without SecretString
      if (cdcSecret === null) {
        secretsMock
          .on(GetSecretValueCommand, { SecretId: baseEnv.CDC_USER_SECRET })
          .resolves({}); // SecretString is undefined => skip CDC
      } else {
        const payload =
          cdcSecret && typeof cdcSecret === 'object'
            ? cdcSecret
            : { username: 'cdc_user', password: 'cdc_password' };
        secretsMock
          .on(GetSecretValueCommand, { SecretId: baseEnv.CDC_USER_SECRET })
          .resolves({ SecretString: JSON.stringify(payload) });
      }
    }

    // --- Stub pg.Clients
    const mainClientStub = {
      // main (no database property passed to constructor)
      database: 'postgres',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      // return "exists" flags depending on the query text
      query: sinon.stub().callsFake(async (stmt: string) => {
        if (stmt.includes('pg_catalog.pg_database')) {
          return { rows: [{ exists: databaseExists }] };
        }
        if (stmt.includes(`rolname='${serviceSecret.username}'`)) {
          return { rows: [{ exists: serviceUserExists }] };
        }
        if (stmt.includes(`rolname='cdc_user'`)) {
          return { rows: [{ exists: cdcUserExists }] };
        }
        // default shape
        return { rows: [{}] };
      }),
    };

    const serviceClientStub = {
      database: derivedDatabase,
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    const cdcClientStub = {
      database: derivedDatabase,
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    // Decide which stub to return per constructor call:
    // 1st -> main, 2nd -> service, 3rd -> CDC (if used)
    let ctorCount = 0;
    const ClientCtor = sinon.stub().callsFake((options: any) => {
      ctorCount += 1;
      if (!options || !('database' in options)) return mainClientStub; // main
      if (ctorCount === 2) return serviceClientStub; // service
      return cdcClientStub; // CDC
    });

    // --- Wire module under test with our stubs
    proxyquire.noCallThru();
    const mod = proxyquire('../../src/bootstrap', {
      pg: { Client: ClientCtor },
      envalid: { cleanEnv: () => baseEnv },
      './bootstrap-validators': { validators: () => ({}), '@noCallThru': true },
    });

    const consoleSpy = sinon.spy(console, 'log');

    return {
      handler: mod,
      ClientCtor,
      mainClientStub,
      serviceClientStub,
      cdcClientStub,
      consoleSpy,
      derivedDatabase,
      derivedSchema,
      serviceSecret,
    };
  }

  // ────────────────────────────────────────────────────────────────────────────
  // 1) No CDC in env: DB+user created as needed, service grants executed
  // ────────────────────────────────────────────────────────────────────────────
  it('creates DB and service user when missing; no CDC path', async () => {
    const {
      handler,
      mainClientStub,
      serviceClientStub,
      cdcClientStub,
      consoleSpy,
    } = setUp(
      { CDC_USER_SECRET: undefined },
      { databaseExists: false, serviceUserExists: false },
    );

    const res = await handler.handler();

    // main path: CREATE DATABASE + CREATE USER
    const mainStatements = mainClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(mainStatements.some(s => s.includes(`SELECT exists(SELECT FROM pg_catalog.pg_database`))).to.be.true;
    expect(mainStatements.some(s => s === `CREATE DATABASE app_database`)).to.be.true;
    expect(mainStatements.some(s => s.includes(`SELECT exists(SELECT FROM pg_roles WHERE rolname='myapp_user')`))).to.be.true;
    expect(mainStatements.some(s => s.includes(`CREATE USER myapp_user WITH ENCRYPTED PASSWORD 'myapp_password'`))).to.be.true;
    // absolutely no CDC user lookup when no CDC secret is configured
    expect(mainStatements.some(s => s.includes(`rolname='cdc_user'`))).to.be.false;

    // service grants: assert presence of all grant/extension statements
    const svc = serviceClientStub.query.getCalls().map(c => c.args[0] as string);
    const expectedFragments = [
      `CREATE SCHEMA IF NOT EXISTS app_schema`,
      `CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA app_schema CASCADE`,
      `CREATE EXTENSION IF NOT EXISTS intarray SCHEMA app_schema CASCADE`,
      `GRANT CONNECT ON DATABASE app_database TO myapp_user`,
      `GRANT CREATE ON DATABASE app_database TO myapp_user`,
      `REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
      `REVOKE ALL ON DATABASE app_database FROM PUBLIC`,
      `GRANT USAGE, CREATE ON SCHEMA app_schema TO myapp_user`,
      `ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user`,
      `GRANT ALL PRIVILEGES on DATABASE app_database to myapp_user`,
      `ALTER DATABASE app_database OWNER TO myapp_user`,
    ];
    expectedFragments.forEach(f =>
      expect(svc.some(s => s === f || s.includes(f)), `missing "${f}"`).to.be.true,
    );
    // "CREATE SCHEMA IF NOT EXISTS ..." is called twice in code – verify duplication
    expect(svc.filter(s => s === `CREATE SCHEMA IF NOT EXISTS app_schema`).length).to.equal(2);

    // CDC connection must not be used at all
    expect(cdcClientStub.connect.called).to.be.false;
    expect(cdcClientStub.query.called).to.be.false;

    // log prefix proves the query() wrapper is used
    expect(consoleSpy.called).to.be.true;
    const logArgs = consoleSpy.getCalls().map(c => String(c.args[0]));
    expect(logArgs.some(l => l.startsWith('[postgres] SELECT exists(SELECT FROM pg_catalog.pg_database'))).to.be.true;
    expect(logArgs.some(l => l.startsWith('[app_database] CREATE SCHEMA IF NOT EXISTS app_schema'))).to.be.true;

    expect(res).to.deep.equal({
      message: `Database 'app_database' usernames are ready for use!`,
    });
  });

  // ────────────────────────────────────────────────────────────────────────────
  // 2) DB and user already exist -> no CREATEs on main
  // ────────────────────────────────────────────────────────────────────────────
  it('skips CREATE DATABASE and CREATE USER when they already exist', async () => {
    const { handler, mainClientStub } = setUp(
      {},
      { databaseExists: true, serviceUserExists: true },
    );

    await handler.handler();

    const mainStatements = mainClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(mainStatements.some(s => s === `CREATE DATABASE app_database`)).to.be.false;
    expect(mainStatements.some(s => s.includes(`CREATE USER myapp_user`))).to.be.false;
  });

  // ────────────────────────────────────────────────────────────────────────────
  // 3) CDC: secret present and user missing -> create user + CDC grants
  // ────────────────────────────────────────────────────────────────────────────
  it('creates CDC user if missing and applies CDC grants', async () => {
    const {
      handler,
      mainClientStub,
      cdcClientStub,
    } = setUp(
      { CDC_USER_SECRET: 'cdc-secret' },
      { cdcUserExists: false }, // force CREATE USER for CDC
      { username: 'cdc_user', password: 'cdc_password' },
    );

    const res = await handler.handler();

    const mainStatements = mainClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(mainStatements.some(s => s.includes(`SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`))).to.be.true;
    expect(mainStatements.some(s => s.includes(`CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'cdc_password'`))).to.be.true;

    const cdcStatements = cdcClientStub.query.getCalls().map(c => c.args[0] as string);
    [
      `GRANT CONNECT ON DATABASE app_database TO cdc_user`,
      `GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user`,
      `GRANT rds_replication, rds_superuser TO cdc_user`,
      `CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES`,
    ].forEach(f =>
      expect(cdcStatements.some(s => s === f || s.includes(f)), `missing CDC "${f}"`).to.be.true,
    );

    expect(res.message).to.equal(`Database 'app_database' usernames are ready for use!`);
  });

  // ────────────────────────────────────────────────────────────────────────────
  // 4) CDC: user already exists -> no CREATE USER, but CDC grants still happen
  // ────────────────────────────────────────────────────────────────────────────
  it('does not create CDC user when it already exists, still applies grants', async () => {
    const {
      handler,
      mainClientStub,
      cdcClientStub,
    } = setUp(
      { CDC_USER_SECRET: 'cdc-secret' },
      { cdcUserExists: true }, // skip CREATE USER for CDC
      { username: 'cdc_user', password: 'cdc_password' },
    );

    await handler.handler();

    const mainStatements = mainClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(mainStatements.some(s => s.includes(`SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`))).to.be.true;
    expect(mainStatements.some(s => s.includes(`CREATE USER cdc_user`))).to.be.false;

    const cdcStatements = cdcClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(cdcStatements.some(s => s.includes(`GRANT CONNECT ON DATABASE app_database TO cdc_user`))).to.be.true;
    expect(cdcStatements.some(s => s.includes(`CREATE PUBLICATION IF NOT EXISTS cdc_publication`))).to.be.true;
  });

  // ────────────────────────────────────────────────────────────────────────────
  // 5) CDC: env contains secret id but AWS returns {SecretString: undefined}
  //    -> whole CDC flow must be skipped
  // ────────────────────────────────────────────────────────────────────────────
  it('skips CDC flow when SecretString is missing/null from Secrets Manager', async () => {
    const {
      handler,
      cdcClientStub,
      mainClientStub,
    } = setUp(
      { CDC_USER_SECRET: 'cdc-secret' },
      {},
      null, // simulate missing SecretString
    );

    await handler.handler();

    // No CDC DB connection or GRANTs
    expect(cdcClientStub.connect.called).to.be.false;
    expect(cdcClientStub.query.called).to.be.false;

    // Also ensure no CDC user lookup was performed on main
    const mainStatements = mainClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(mainStatements.some(s => s.includes(`rolname='cdc_user'`))).to.be.false;
  });

  // ────────────────────────────────────────────────────────────────────────────
  // 6) Defaults for database & schema when APP_DATABASE_NAME/SCHEMA are undefined
  // ────────────────────────────────────────────────────────────────────────────
  it('derives database and schema from service secret when env values are undefined', async () => {
    const {
      handler,
      ClientCtor,
      serviceClientStub,
    } = setUp(
      { APP_DATABASE_NAME: undefined, APP_SCHEMA_NAME: undefined, CDC_USER_SECRET: undefined },
      { databaseExists: false, serviceUserExists: false },
    );

    const res = await handler.handler();

    // 2nd constructor call is the service connection; check the database it was given
    const ctorArgs = ClientCtor.getCall(1).args[0];
    expect(ctorArgs.database).to.equal('myapp'); // derived from "myapp_user".replace('_user','')

    // service schema falls back to username
    const svcStatements = serviceClientStub.query.getCalls().map(c => c.args[0] as string);
    expect(svcStatements.filter(s => s === `CREATE SCHEMA IF NOT EXISTS myapp_user`).length).to.equal(2);
    expect(svcStatements.some(s => s.includes(`GRANT USAGE, CREATE ON SCHEMA myapp_user TO myapp_user`))).to.be.true;

    // message uses derived database as well
    expect(res).to.deep.equal({
      message: `Database 'myapp' usernames are ready for use!`,
    });
  });
});

