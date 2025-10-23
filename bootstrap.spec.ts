
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

describe('bootstrap.handler - 100% Code & Branch Coverage', () => {
  const secretsMock = mockClient(SecretsManagerClient);

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  function setup(
    envOverrides: Record<string, any> = {},
    existence: { databaseExists?: boolean; serviceUserExists?: boolean; cdcUserExists?: boolean } = {},
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

    // Secrets mocks
    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.MASTER_USER_SECRET })
      .resolves({
        SecretString: JSON.stringify({ username: 'admin', password: 'pw' }),
      });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.APP_USER_SECRET })
      .resolves({
        SecretString: JSON.stringify({ username: 'myapp_user', password: 'mypw' }),
      });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {}
          : { SecretString: JSON.stringify(cdcSecret ?? { username: 'cdc_user', password: 'cdc_pw' }) };
      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    const main = {
      database: 'postgres',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake(async (sql: string) => {
        if (simulateError === 'main') {
          await new Promise((r) => setTimeout(r, 5));
          throw new Error('main query failed');
        }
        if (sql.includes('pg_catalog.pg_database')) return { rows: [{ exists: databaseExists }] };
        if (sql.includes("rolname='myapp_user'")) return { rows: [{ exists: serviceUserExists }] };
        if (sql.includes("rolname='cdc_user'")) return { rows: [{ exists: cdcUserExists }] };
        return { rows: [{}] };
      }),
    };

    const service = {
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'service') return Promise.reject(new Error('service query fail'));
        return { rows: [{}] };
      }),
    };

    const cdc = {
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'cdc') return Promise.reject(new Error('cdc query fail'));
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

    const consoleErr = sinon.stub(console, 'error');

    const mod = proxyquire('../../src/bootstrap', {
      pg: { Client: ClientStub },
      envalid: { cleanEnv: () => env },
    });

    return { handler: mod.handler, main, service, cdc, ctorArgs, consoleErr };
  }

  // --- Positive Base Path ---
  it('runs happy path without CDC', async () => {
    const { handler, main, service } = setup();
    const res = await handler();
    expect(res.message).to.include('app_db');
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
  });

  // --- Fallback for DB & Schema ---
  it('applies fallback when APP_DATABASE_NAME and APP_SCHEMA_NAME undefined', async () => {
    const { handler, service, ctorArgs } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
    const res = await handler();
    expect(res.message).to.include('myapp');
    expect(ctorArgs[1].database).to.equal('myapp');
    expect(service.query.callCount).to.be.greaterThan(5);
  });

  // --- CDC user creation ---
  it('creates CDC user and applies CDC grants', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    expect(main.query.callCount).to.be.greaterThan(3);
    expect(cdc.query.callCount).to.be.greaterThan(3);
    expect(cdc.end.calledOnce).to.be.true;
  });

  // --- CDC existing user (no creation) ---
  it('skips CDC user creation if already exists', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    expect(main.query.getCalls().map(c => c.args[0]).join()).to.include("rolname='cdc_user'");
    expect(cdc.query.callCount).to.be.greaterThan(1);
  });

  // --- CDC SecretString null ---
  it('skips CDC flow when SecretString is null', async () => {
    const { handler, cdc } = setup({ CDC_USER_SECRET: 'cdc' }, {}, null);
    await handler();
    expect(cdc.connect.called).to.be.false;
    expect(cdc.query.called).to.be.false;
  });

  // --- CDC Secret undefined (completely skipped) ---
  it('skips CDC when env.CDC_USER_SECRET undefined', async () => {
    const { handler, cdc } = setup();
    await handler();
    expect(cdc.connect.called).to.be.false;
  });

  // --- MainConn catch block ---
  it('logs error in mainConn catch when query throws', async () => {
    const { handler, main, consoleErr } = setup({}, {}, undefined, 'main');
    await handler();
    expect(consoleErr.calledOnce).to.be.true;
    expect(main.end.calledOnce).to.be.true;
  });

  // --- Service query throws ---
  it('executes finally for service query throw', async () => {
    const { handler, main, service } = setup({}, {}, undefined, 'service');
    await handler().catch(() => {});  // swallow rejection
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
  });

  // --- CDC query throws ---
  it('executes finally for CDC query throw', async () => {
    const { handler, main, service, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      {},
      { username: 'cdc_user', password: 'cdc_pw' },
      'cdc',
    );
    await handler().catch(() => {});  // swallow rejection
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
    expect(cdc.end.calledOnce).to.be.true;
  });

  // --- Database & service user existence toggles ---
  it('creates DB only when missing and user when missing', async () => {
    const { handler, main } = setup({}, { databaseExists: false, serviceUserExists: false });
    await handler();
    const sql = main.query.getCalls().map(c => c.args[0]).join(' ');
    expect(sql).to.include('CREATE DATABASE');
    expect(sql).to.include('CREATE USER myapp_user');
  });

  it('skips DB & user creation when already exists', async () => {
    const { handler, main } = setup({}, { databaseExists: true, serviceUserExists: true });
    await handler();
    const sql = main.query.getCalls().map(c => c.args[0]).join(' ');
    expect(sql).not.to.include('CREATE DATABASE');
    expect(sql).not.to.include('CREATE USER myapp_user');
  });

  // --- Return message ---
  it('always returns the constant success message', async () => {
    const { handler } = setup();
    const res = await handler();
    expect(res).to.deep.equal({
      message: "Database 'app_db' usernames are ready for use!",
    });
  });
  
  it('logs error and still executes finally when service query throws', async () => {
    const { handler, service } = setup({}, {}, undefined, 'service');
    const consoleErr = sinon.stub(console, 'error');
  
    await handler(); // no .catch needed since handler no longer throws
  
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.serviceConn] query failure:');
    expect(service.end.calledOnce).to.be.true;
  });

  it('logs error and executes finally when CDC query throws', async () => {
    const { handler, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      {},
      { username: 'cdc_user', password: 'cdc_pw' },
      'cdc',
    );
    const consoleErr = sinon.stub(console, 'error');
  
    await handler();
  
    expect(consoleErr.calledOnce).to.be.true;
    expect(consoleErr.firstCall.args[0]).to.include('[bootstrap.cdcDbConn] query failure:');
    expect(cdc.end.calledOnce).to.be.true;
  });
});
