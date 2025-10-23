
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

describe('bootstrap.handler - Full Coverage Suite', () => {
  const secretsMock = mockClient(SecretsManagerClient);

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  /**
   * Controlled setup that covers every branch:
   * - envOverrides: overrides APP/DB/SCHEMA/CDS settings
   * - existence: toggles database/user existence flags
   * - cdcSecret: sets CDC secret payload, null, or undefined
   * - simulateError: 'main' | 'service' | 'cdc' for rejection flows
   */
  function setup(
    envOverrides: Partial<Record<string, any>> = {},
    existence: { databaseExists?: boolean; serviceUserExists?: boolean; cdcUserExists?: boolean } = {},
    cdcSecret: { username?: string; password?: string } | null | undefined = undefined,
    simulateError?: 'main' | 'service' | 'cdc',
  ) {
    const env = {
      MASTER_USER_SECRET: 'master-secret',
      APP_USER_SECRET: 'app-secret',
      CDC_USER_SECRET: undefined,
      APP_DATABASE_NAME: 'app_database',
      APP_SCHEMA_NAME: 'app_schema',
      RDS_HOST: 'host',
      ...envOverrides,
    };

    // Recommended approach — no destructuring warning
    const databaseExists = existence.databaseExists ?? false;
    const serviceUserExists = existence.serviceUserExists ?? false;
    const cdcUserExists = existence.cdcUserExists ?? false;

    // Secrets Manager mocks
    secretsMock
      .on(GetSecretValueCommand, { SecretId: env.MASTER_USER_SECRET })
      .resolves({
        SecretString: JSON.stringify({ username: 'admin', password: 'admin_pw' }),
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

    // pg stubs
    const main = {
      database: 'postgres',
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'main') return Promise.reject(new Error('main query failed'));
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
        if (simulateError === 'service') return Promise.reject(new Error('service query failed'));
        return { rows: [{}] };
      }),
    };

    const cdc = {
      connect: sinon.stub().resolves(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql: string) => {
        if (simulateError === 'cdc') return Promise.reject(new Error('cdc query failed'));
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

  // --------------------------------------------------------------------------
  // Tests: Positive and fallback flows
  // --------------------------------------------------------------------------

  it('creates DB + user + grants correctly when all missing', async () => {
    const { handler, main, service, ctorArgs } = setup();
    const res = await handler();
    expect(res.message).to.equal("Database 'app_database' usernames are ready for use!");
    expect(main.query.callCount).to.be.at.least(4);
    expect(service.query.callCount).to.be.greaterThan(8);
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
    expect(ctorArgs[0]).to.include.keys('user', 'password', 'host', 'port');
    expect(ctorArgs[1]).to.include.keys('database', 'user', 'password');
  });

  it('skips DB + user creation when already exists', async () => {
    const { handler, main } = setup({}, { databaseExists: true, serviceUserExists: true });
    await handler();
    const sqls = main.query.getCalls().map((c) => c.args[0]);
    expect(sqls.join()).not.to.include('CREATE DATABASE');
    expect(sqls.join()).not.to.include('CREATE USER myapp_user');
  });

  it('handles explicit schema correctly (kills schema logical-operator mutant)', async () => {
    const { handler, service } = setup({ APP_SCHEMA_NAME: 'app_schema' });
    await handler();
    const sqls = service.query.getCalls().map((c) => c.args[0]);
    expect(sqls.join()).to.include('CREATE SCHEMA IF NOT EXISTS app_schema');
  });

  it('falls back to username-derived DB + schema when env undefined', async () => {
    const { handler, service, ctorArgs } = setup({
      APP_DATABASE_NAME: undefined,
      APP_SCHEMA_NAME: undefined,
    });
    const res = await handler();
    expect(res.message).to.equal("Database 'myapp' usernames are ready for use!");
    const sqls = service.query.getCalls().map((c) => c.args[0]);
    expect(sqls.join()).to.include('myapp_user');
    expect(ctorArgs[1]).to.include({ database: 'myapp' });
  });

  // --------------------------------------------------------------------------
  // CDC Branches
  // --------------------------------------------------------------------------

  it('creates CDC user and applies grants/publication when missing', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    expect(main.query.called).to.be.true;
    const cdcSQL = cdc.query.getCalls().map((c) => c.args[0]);
    expect(cdcSQL.join()).to.include('CREATE PUBLICATION');
    expect(cdc.end.calledOnce).to.be.true;
  });

  it('skips CDC user creation if already exists but runs grants', async () => {
    const { handler, main, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pw' },
    );
    await handler();
    const mainSQL = main.query.getCalls().map((c) => c.args[0]);
    expect(mainSQL.join()).not.to.include('CREATE USER cdc_user');
    expect(cdc.query.callCount).to.be.greaterThan(3);
  });

  it('skips CDC flow entirely when SecretString null', async () => {
    const { handler, cdc } = setup({ CDC_USER_SECRET: 'cdc' }, {}, null);
    await handler();
    expect(cdc.connect.called).to.be.false;
    expect(cdc.query.called).to.be.false;
  });

  it('handles malformed CDC secret gracefully (username missing)', async () => {
    const { handler, cdc } = setup({ CDC_USER_SECRET: 'cdc' }, {}, { password: 'pw-only' });
    await handler();
    expect(cdc.connect.called).to.be.true;
    expect(cdc.query.callCount).to.be.greaterThan(0);
  });

  // --------------------------------------------------------------------------
  // Exception / finally-block coverage
  // --------------------------------------------------------------------------

  it('ensures .end() calls execute even if main query throws', async () => {
    const { handler, main, service } = setup({}, {}, undefined, 'main');
    await handler().catch(() => {});
    await new Promise((r) => setImmediate(r));
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
  });

  it('ensures .end() calls execute even if service query throws', async () => {
    const { handler, main, service } = setup({}, {}, undefined, 'service');
    await handler().catch(() => {});
    await new Promise((r) => setImmediate(r));
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
  });

  it('ensures .end() calls execute even if cdc query throws', async () => {
    const { handler, main, service, cdc } = setup(
      { CDC_USER_SECRET: 'cdc' },
      {},
      { username: 'cdc_user', password: 'cdc_pw' },
      'cdc',
    );
    await handler().catch(() => {});
    await new Promise((r) => setImmediate(r));
    expect(main.end.calledOnce).to.be.true;
    expect(service.end.calledOnce).to.be.true;
    expect(cdc.end.calledOnce).to.be.true;
  });

  // --------------------------------------------------------------------------
  // Message return validation
  // --------------------------------------------------------------------------

  it('always returns the constant success message', async () => {
    const { handler } = setup({ CDC_USER_SECRET: 'cdc' }, {}, { username: 'cdc_user', password: 'cdc_pw' });
    const res = await handler();
    expect(res).to.deep.equal({
      message: "Database 'app_database' usernames are ready for use!",
    });
  });
});
