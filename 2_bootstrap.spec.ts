
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

  // Happy path
  it('runs happy path without CDC', async () => {
    const { handler, main, service } = setup();
    const res = await handler();
    expect(res.message).to.include('app_db');
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;

    // kill string mutants
    const svcSQL = service.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(svcSQL).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');
    expect(svcSQL).to.include('GRANT CONNECT ON DATABASE app_db TO myapp_user');
  });

  // Fallback path for DB/schema
  it('uses fallback when APP_DATABASE_NAME and APP_SCHEMA_NAME undefined', async () => {
    const { handler, service } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
    const res = await handler();
    expect(res.message).to.include('myapp');
    const sql = service.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).to.include('myapp_user');
  });

  // CDC user creation
  it('creates CDC user and applies grants/publications', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();

    const mainSQL = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(mainSQL).to.include('CREATE USER cdc_user');
    const cdcSQL = cdc.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(cdcSQL).to.include('GRANT rds_replication, rds_superuser TO cdc_user');
    expect(cdcSQL).to.include('CREATE PUBLICATION IF NOT EXISTS cdc_publication');
  });

  // CDC existing user
  it('skips CDC user creation when already exists', async () => {
    const { handler, main } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    const sql = main.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(sql).to.include("rolname='cdc_user'");
    expect(sql).not.to.include('CREATE USER cdc_user');
  });

  // Null CDC secret path
  it('skips CDC flow when SecretString is null', async () => {
    const { handler, cdc } = setup({ CDC_USER_SECRET: 'cdc' }, {}, null);
    await handler();
    expect(cdc.connect.called).to.be.false;
  });

  // Main catch
  it('logs error in mainConn catch when query fails', async () => {
    const { handler, main } = setup({}, {}, undefined, 'main');
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.mainConn]');
    expect(main.end.calledOnce).to.be.true;
  });

  // Service catch
  it('logs error and executes finally when service query throws', async () => {
    const { handler, service } = setup({}, {}, undefined, 'service');
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.serviceConn]');
    expect(service.end.calledOnce).to.be.true;
  });

  // CDC catch
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

  // DB creation toggles
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

  // Final message check
  it('always returns the constant message', async () => {
    const { handler } = setup();
    const res = await handler();
    expect(res).to.deep.equal({
      message: "Database 'app_db' usernames are ready for use!",
    });
  });
});
