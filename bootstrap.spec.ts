
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

describe('Handler - Unit', () => {
  let secretsMock;
  before(() => (secretsMock = mockClient(SecretsManagerClient)));
  afterEach(() => {
    secretsMock.reset();
    sinon.restore();
  });

  function setUp(
    envOverrides = {},
    existenceOverrides: ExistenceOverrides = {},
    cdcSecret: { username?: string; password?: string } | null | undefined = undefined,
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

    // --- Secrets mocks ---
    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'master-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'admin_user', password: 'admin_password' }) });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'app-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'myapp_user', password: 'myapp_password' }) });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {}
          : { SecretString: JSON.stringify(cdcSecret ?? { username: 'cdc_user', password: 'cdc_password' }) };
      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql) => {
        if (sql.includes('pg_catalog.pg_database')) return { rows: [{ exists: databaseExists }] };
        if (sql.includes(`rolname='myapp_user'`)) return { rows: [{ exists: serviceUserExists }] };
        if (sql.includes(`rolname='cdc_user'`)) return { rows: [{ exists: cdcUserExists }] };
        return { rows: [{}] };
      }),
    };

    const serviceClientStub = {
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
      database: appDatabase,
    };

    const cdcClientStub = {
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().resolves({ rows: [{}] }),
      database: appDatabase,
    };

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

    return { handler, mainClientStub, serviceClientStub, cdcClientStub };
  }

  const expectedMsg = "Database 'app_database' usernames are ready for use!";

  // -----------------------------------------------------------------------
  it('handles base path with no CDC secret', async () => {
    const { handler } = setUp();
    const result = await handler.handler();
    expect(result.message).to.equal(expectedMsg);
  });

  it('creates CDC user and full grants when user does not exist', async () => {
    const { handler, mainClientStub, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    const result = await handler.handler();
    const sqls = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(sqls.some((s) => s.includes('CREATE USER cdc_user'))).to.be.true;
    const cdcSql = cdcClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(cdcSql).to.include('GRANT CONNECT');
    expect(cdcSql).to.include('GRANT SELECT');
    expect(cdcSql).to.include('CREATE PUBLICATION');
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC user creation if already exists', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    const result = await handler.handler();
    const queries = mainClientStub.query.getCalls().map((q) => q.args[0]);
    expect(queries.join()).to.include("SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')");
    expect(queries.join()).to.not.include('CREATE USER cdc_user');
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC flow entirely when SecretString is null', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, null);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC if username missing in secret', async () => {
    const { handler, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { password: 'only-pass' },
    );
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC when env var is not provided', async () => {
    const { handler, cdcClientStub } = setUp({}, {}, undefined);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips DB/user creation when both already exist', async () => {
    const { handler, mainClientStub } = setUp({}, { databaseExists: true, serviceUserExists: true });
    await handler.handler();
    const calls = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(calls.filter((c) => c.includes('CREATE')).length).to.equal(0);
  });

  it('handles partial cdcSecret (only username)', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { username: 'partial_user' },
    );
    const result = await handler.handler();
    const calls = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(calls.join()).to.include("rolname='partial_user'");
    expect(result.message).to.equal(expectedMsg);
  });
});
