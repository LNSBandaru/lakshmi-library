
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from '@aws-sdk/client-secrets-manager';
import { mockClient } from 'aws-sdk-client-mock';
import { expect } from 'chai';
import * as proxyquire from 'proxyquire';
import * as sinon from 'sinon';

describe('Handler - Unit', () => {
  let secretsMock;
  before(() => {
    secretsMock = mockClient(SecretsManagerClient);
  });

  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  function setUp(
    envOverrides = {},
    existenceOverrides = {},
    cdcSecret: any = undefined
  ) {
    const env = {
      MASTER_USER_SECRET: 'master-test',
      APP_USER_SECRET: 'app-test',
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

    const appDatabase = env.APP_DATABASE_NAME ?? 'myapp';

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'master-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'admin_user', password: 'admin_password' }) });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'app-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'myapp_user', password: 'myapp_password' }) });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {} // simulate missing SecretString
          : { SecretString: JSON.stringify(cdcSecret || { username: 'cdc_user', password: 'cdc_password' }) };
      secretsMock.on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET }).resolves(payload);
    }

    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((stmt) => {
        if (stmt.includes('pg_catalog.pg_database')) return { rows: [{ exists: databaseExists }] };
        if (stmt.includes(`rolname='myapp_user'`)) return { rows: [{ exists: serviceUserExists }] };
        if (stmt.includes(`rolname='cdc_user'`)) return { rows: [{ exists: cdcUserExists }] };
        return { rows: [{}] };
      }),
    };

    const serviceClientStub = {
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      database: appDatabase,
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    const cdcClientStub = {
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      database: appDatabase,
      query: sinon.stub().resolves({ rows: [{}] }),
    };

    let ctorCount = 0;
    const pgStub = sinon.stub().callsFake((options) => {
      ctorCount++;
      if (!options.database) return mainClientStub;
      if (options.database === appDatabase && ctorCount === 2) return serviceClientStub;
      if (options.database === appDatabase && ctorCount === 3 && env.CDC_USER_SECRET) return cdcClientStub;
      return serviceClientStub;
    });

    const handler = proxyquire('../../src/bootstrap', {
      pg: { Client: pgStub },
      envalid: { cleanEnv: () => env },
    });

    const consoleSpy = sinon.spy(console, 'log');
    return {
      handler,
      pgStub,
      mainClientStub,
      serviceClientStub,
      cdcClientStub,
      consoleSpy,
    };
  }

  it('returns non-CDC message when CDC secret is undefined', async () => {
    const { handler } = setUp();
    const result = await handler.handler();
    expect(result).to.deep.equal({
      message: `Database 'app_database' usernames are ready for use!`,
    });
  });

  it('returns CDC message when CDC secret is valid', async () => {
    const { handler } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, { username: 'cdc_user', password: 'cdc_password' });
    const result = await handler.handler();
    expect(result).to.deep.equal({
      message: `Database 'app_database' usernames are ready for use!`,
    });
  });

  it('should skip CDC if SecretString is null', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, null);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(`Database 'app_database' usernames are ready for use!`);
  });

  it('should skip CDC if username is undefined', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, { password: 'no-user' });
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(`Database 'app_database' usernames are ready for use!`);
  });

  it('should create CDC user and grants when username exists', async () => {
    const { handler, mainClientStub, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_password' }
    );
    const result = await handler.handler();
    expect(mainClientStub.query.getCall(4).args[0]).to.include(`rolname='cdc_user'`);
    expect(cdcClientStub.query.getCall(0).args[0]).to.include(`GRANT CONNECT`);
    expect(result.message).to.equal(`Database 'app_database' usernames are ready for use!`);
  });

  it('should skip CDC user creation if already exists', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' }
    );
    const result = await handler.handler();
    const queries = mainClientStub.query.getCalls().map((q) => q.args[0]);
    expect(queries).to.include(`SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`);
    expect(queries).to.not.include(`CREATE USER cdc_user`);
    expect(result.message).to.equal(`Database 'app_database' usernames are ready for use!`);
  });
});

