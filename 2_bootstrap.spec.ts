
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

describe('bootstrap.handler - 100% Code & Mutation Coverage', () => {
  const secretsMock = mockClient(SecretsManagerClient);
  let consoleErr: sinon.SinonStub;

  beforeEach(() => {
    consoleErr = sinon.stub(console, 'error');
  });

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  function setup(
    envOverrides: Record<string, any> = {},
    existence: {
      databaseExists?: boolean;
      serviceUserExists?: boolean;
      cdcUserExists?: boolean;
    } = {},
    cdcSecret: any = undefined,
    simulateError?: 'main' | 'service' | 'cdc',
  ) {
    const env = {
      MASTER_USER_SECRET: 'master',
      APP_USER_SECRET: 'app',
      CDC_USER_SECRET: undefined,
      APP_DATABASE_NAME: 'app_db',
      APP_SCHEMA_NAME: 'app_schema',
      RDS_HOST: 'localhost',
      ...envOverrides,
    };

    const databaseExists = existence.databaseExists ?? false;
    const serviceUserExists = existence.serviceUserExists ?? false;
    const cdcUserExists = existence.cdcUserExists ?? false;

    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.MASTER_USER_SECRET })
      .resolves({
        SecretString: JSON.stringify({ username: 'admin', password: 'pw' }),
      });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.APP_USER_SECRET })
      .resolves({
        SecretString: JSON.stringify({
          username: 'myapp_user',
          password: 'mypw',
        }),
      });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {}
          : {
              SecretString: JSON.stringify(
                cdcSecret ?? { username: 'cdc_user', password: 'cdc_pw' },
              ),
            };
      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    const main = {
      database: 'postgres',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake(async (sql: string) => {
        if (simulateError === 'main') throw new Error('main query failed');
        if (sql.includes('pg_catalog.pg_database'))
          return { rows: [{ exists: databaseExists }] };
        if (sql.includes("rolname='myapp_user'"))
          return { rows: [{ exists: serviceUserExists }] };
        if (sql.includes("rolname='cdc_user'"))
          return { rows: [{ exists: cdcUserExists }] };
        return { rows: [{}] };
      }),
    };

    const service = {
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'service')
          return Promise.reject(new Error('service query fail'));
        return { rows: [{}] };
      }),
    };

    const cdc = {
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'cdc')
          return Promise.reject(new Error('cdc query fail'));
        return { rows: [{}] };
      }),
    };

    const ctorArgs: any[] = [];
    let call = 0;
    const ClientStub = sinon.stub().callsFake((opts: any) => {
      ctorArgs.push(opts);
      call++;
      if (!opts.database) return main;
      if (env.CDC_USER_SECRET && call >= 3) return cdc;
      return service;
    });

    const mod = proxyquire('../../src/bootstrap', {
      pg: { Client: ClientStub },
      envalid: { cleanEnv: () => env },
    });

    return { handler: mod.handler, main, service, cdc, ctorArgs };
  }

  // --- Verify full constructor coverage
  it('constructs mainConn with correct parameters', async () => {
    const { handler, ctorArgs } = setup();
    await handler();
    const mainCtor = ctorArgs[0];
    expect(mainCtor.user).to.equal('admin');
    expect(mainCtor.password).to.equal('pw');
    expect(mainCtor.host).to.equal('localhost');
    expect(mainCtor.port).to.equal(5432);
  });

  // --- Happy path
  it('runs happy path without CDC and validates SQL', async () => {
    const { handler, service } = setup();
    const res = await handler();
    expect(res.message).to.include('app_db');
    const sqls = service.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sqls).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');
    expect(sqls).to.include('CREATE EXTENSION IF NOT EXISTS pg_trgm');
    expect(sqls).to.include('CREATE EXTENSION IF NOT EXISTS intarray');
    expect(sqls).to.include('GRANT CREATE ON DATABASE app_db TO myapp_user');
    expect(sqls).to.include('ALTER DATABASE app_db OWNER TO myapp_user');
  });

  // --- Fallback path for DB/schema
  it('uses fallback when APP_DATABASE_NAME and APP_SCHEMA_NAME undefined', async () => {
    const { handler, service } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
    const res = await handler();
    expect(res.message).to.include('myapp');
    const sql = service.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).to.include('myapp_user');
    expect(sql).to.include('CREATE SCHEMA IF NOT EXISTS myapp_user');
  });

  // --- CDC user creation
  it('creates CDC user and applies full grants/publications', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    const mainSQL = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(mainSQL).to.include('CREATE USER cdc_user');
    const cdcSQL = cdc.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(cdcSQL).to.include('GRANT CONNECT ON DATABASE app_db TO cdc_user');
    expect(cdcSQL).to.include('GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user');
    expect(cdcSQL).to.include('GRANT rds_replication, rds_superuser TO cdc_user');
    expect(cdcSQL).to.include('CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES');
  });

  // --- CDC existing user
  it('skips CDC user creation when already exists', async () => {
    const { handler, main } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    const sql = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).not.to.include('CREATE USER cdc_user');
  });

  // --- Null CDC secret
  it('skips CDC flow when SecretString is null', async () => {
    const { handler, cdc } = setup({ CDC_USER_SECRET: 'cdc' }, {}, null);
    await handler();
    expect(cdc.connect.called).to.be.false;
  });

  // --- Main catch
  it('logs error in mainConn catch', async () => {
    const { handler, main } = setup({}, {}, undefined, 'main');
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.mainConn]');
    expect(main.end.calledOnce).to.be.true;
  });

  // --- Service catch
  it('logs error and executes finally when service query throws', async () => {
    const { handler, service } = setup({}, {}, undefined, 'service');
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.serviceConn]');
    expect(service.end.calledOnce).to.be.true;
  });

  // --- CDC catch
  it('logs error and executes finally when CDC query throws', async () => {
    const { handler, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      {},
      { username: 'cdc_user', password: 'cdc_pw' },
      'cdc',
    );
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.cdcDbConn]');
    expect(cdc.end.calledOnce).to.be.true;
  });

  // --- Database creation toggles
  it('creates DB and user only when missing', async () => {
    const { handler, main } = setup(
      {},
      { databaseExists: false, serviceUserExists: false },
    );
    await handler();
    const sql = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).to.include('CREATE DATABASE');
    expect(sql).to.include('CREATE USER myapp_user');
  });

  it('skips DB and user creation when already exists', async () => {
    const { handler, main } = setup(
      {},
      { databaseExists: true, serviceUserExists: true },
    );
    await handler();
    const sql = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).not.to.include('CREATE DATABASE');
    expect(sql).not.to.include('CREATE USER myapp_user');
  });

  // --- Final message
  it('always returns the constant message', async () => {
    const { handler } = setup();
    const res = await handler();
    expect(res).to.deep.equal({
      message: "Database 'app_db' usernames are ready for use!",
    });
  });
  
  it('derives database name by replacing "_user" in service username', async () => {
    const { handler, service, ctorArgs } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
  
    const res = await handler();
    expect(res.message).to.include('myapp'); // fallback check
    // explicit verification of the replacement logic
    const dbArg = ctorArgs[1].database;
    expect(dbArg).to.equal('myapp');           // confirms "_user" removed
    expect(dbArg).not.to.include('_user');     // kills both 56:61 and 56:70 string mutants
  });

  it('executes REVOKE CREATE ON SCHEMA public FROM PUBLIC', async () => {
    const { handler, service } = setup();
    await handler();
    const sqls = service.query.getCalls().map(c => c.args[0]);
    // verify exact revoke string used
    const revokeStmt = sqls.find(q => q.includes('REVOKE CREATE ON SCHEMA public FROM PUBLIC'));
    expect(revokeStmt).to.not.be.undefined;
    expect(revokeStmt).to.include('REVOKE CREATE ON SCHEMA public FROM PUBLIC');
  });

  it('derives database name by replacing "_user" in service username', async () => {
    const { handler, service, ctorArgs } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
  
    const res = await handler();
    expect(res.message).to.include('myapp');
    expect(service.query.called).to.be.true; // now 'service' is used
    const dbArg = ctorArgs[1].database;
    expect(dbArg).to.equal('myapp');
    expect(dbArg).not.to.include('_user');
  });

  it('executes all key serviceConn SQL statements including CREATE and GRANT queries', async () => {
  const { handler, service } = setup();
  await handler();

  // collect executed SQL queries
  const queries = service.query.getCalls().map(c => c.args[0]).join(' ');

  // Explicitly verify the exact statements so Stryker cannot mutate them away
  expect(queries).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');
  expect(queries).to.include('CREATE EXTENSION IF NOT EXISTS pg_trgm');
  expect(queries).to.include('CREATE EXTENSION IF NOT EXISTS intarray');
  expect(queries).to.include('GRANT CREATE ON DATABASE app_db TO myapp_user');
  expect(queries).to.include('ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user');
  expect(queries).to.include('ALTER DATABASE app_db OWNER TO myapp_user');
});

  it('executes all expected SQL commands in serviceConn block (CREATE, GRANT, REVOKE)', async () => {
  const { handler, service } = setup();
  await handler();

  // Collect all SQL executed by serviceConn
  const queries = service.query.getCalls().map(c => c.args[0]).join(' ');

  // Verify CREATE SCHEMA commands
  expect(queries).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');

  // Verify GRANT statements
  expect(queries).to.include('GRANT CONNECT ON DATABASE app_db TO myapp_user');
  expect(queries).to.include('GRANT CREATE ON DATABASE app_db TO myapp_user');
  expect(queries).to.include('GRANT ALL PRIVILEGES on DATABASE app_db to myapp_user');

  // Verify REVOKE statement
  expect(queries).to.include('REVOKE ALL ON DATABASE app_db FROM PUBLIC');

  // Verify ALTER statements
  expect(queries).to.include('ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user');
  expect(queries).to.include('ALTER DATABASE app_db OWNER TO myapp_user');
});

  // ----
  it('executes all expected SQL statements in serviceConn (CREATE, GRANT, REVOKE)', async () => {
  const { handler, service } = setup();
  await handler();

  // Collect all executed SQL statements
  const queries = service.query.getCalls().map(c => c.args[0]).join(' ');

  // ✅ Verify each SQL literal explicitly
  expect(queries).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');
  expect(queries).to.include('GRANT CONNECT ON DATABASE app_db TO myapp_user');
  expect(queries).to.include('CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA app_schema CASCADE');
  expect(queries).to.include('CREATE EXTENSION IF NOT EXISTS intarray SCHEMA app_schema CASCADE');
  expect(queries).to.include('REVOKE ALL ON DATABASE app_db FROM PUBLIC');
  expect(queries).to.include('GRANT USAGE, CREATE ON SCHEMA app_schema TO myapp_user');
  expect(queries).to.include(
    'ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user'
  );
  expect(queries).to.include('GRANT ALL PRIVILEGES on DATABASE app_db to myapp_user');
  expect(queries).to.include('ALTER DATABASE app_db OWNER TO myapp_user');
});

  it('uses explicit schema when APP_SCHEMA_NAME is provided', async () => {
  const { handler, service } = setup({ APP_SCHEMA_NAME: 'custom_schema' });
  await handler();
  const sqls = service.query.getCalls().map(c => c.args[0]).join(' ');
  expect(sqls).to.include('CREATE SCHEMA IF NOT EXISTS custom_schema');
});

  
  // ----

});
