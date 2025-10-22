
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

describe('Handler - 100% Mutation Coverage', () => {
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
    const appSchema = env.APP_SCHEMA_NAME ?? 'myapp_user';

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'master-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'admin_user', password: 'admin_pass' }) });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'app-test' })
      .resolves({ SecretString: JSON.stringify({ username: 'myapp_user', password: 'myapp_pass' }) });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {}
          : { SecretString: JSON.stringify(cdcSecret ?? { username: 'cdc_user', password: 'cdc_pass' }) };
      secretsMock.on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET }).resolves(payload);
    }

    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql) => {
        if (sql.includes('pg_catalog.pg_database')) return { rows: [{ exists: databaseExists }] };
        if (sql.includes(`rolname='myapp_user'`)) return { rows: [{ exists: serviceUserExists }] };
        if (sql.includes(`rolname='cdc_user'`) || sql.includes(`rolname='partial_user'`))
          return { rows: [{ exists: cdcUserExists }] };
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
    const pgStub = sinon.stub().callsFake((opts: any) => {
      ctorCount++;
      if (!opts.database) return mainClientStub;
      if (opts.database === appDatabase && ctorCount === 2) return serviceClientStub;
      if (opts.database === appDatabase && ctorCount === 3 && env.CDC_USER_SECRET)
        return cdcClientStub;
      return serviceClientStub;
    });

    const handler = proxyquire('../../src/bootstrap', {
      pg: { Client: pgStub },
      envalid: { cleanEnv: () => env },
    });

    return { handler, env, mainClientStub, serviceClientStub, cdcClientStub, appSchema };
  }

  const expectedMsg = "Database 'app_database' usernames are ready for use!";

  // ---------------------------------------------------------
  // Non-CDC paths
  it('returns message without CDC secret (baseline)', async () => {
    const { handler } = setUp();
    const result = await handler.handler();
    expect(result).to.deep.equal({ message: expectedMsg });
  });

  it('skips CDC flow when CDC_USER_SECRET is undefined', async () => {
    const { handler, cdcClientStub } = setUp({}, {}, undefined);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('uses default schema when APP_SCHEMA_NAME undefined', async () => {
    const { handler, serviceClientStub, appSchema } = setUp({ APP_SCHEMA_NAME: undefined });
    await handler.handler();
    const calls = serviceClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(calls).to.include(`CREATE SCHEMA IF NOT EXISTS ${appSchema}`);
  });

  it('skips DB/user creation when both already exist', async () => {
    const { handler, mainClientStub } = setUp({}, { databaseExists: true, serviceUserExists: true });
    await handler.handler();
    const queries = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(queries.filter((q) => q.includes('CREATE')).length).to.equal(0);
  });

  // ---------------------------------------------------------
  // CDC branch coverage
  it('creates CDC user and grants when not existing', async () => {
    const { handler, mainClientStub, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_pass' },
    );
    const result = await handler.handler();
    const mainSQL = mainClientStub.query.getCalls().map((q) => q.args[0]).join(' ');
    expect(mainSQL).to.include('CREATE USER cdc_user');
    const cdcSQL = cdcClientStub.query.getCalls().map((q) => q.args[0]).join(' ');
    expect(cdcSQL).to.include('GRANT CONNECT');
    expect(cdcSQL).to.include('CREATE PUBLICATION');
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC user creation if user already exists', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pass' },
    );
    const result = await handler.handler();
    const allSQL = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(allSQL).to.include("rolname='cdc_user'");
    expect(allSQL).to.not.include('CREATE USER cdc_user');
    expect(result.message).to.equal(expectedMsg);
  });

  it('executes CDC block when username missing (object truthy)', async () => {
    const { handler, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { password: 'no-username' },
    );
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.true;
    const calls = cdcClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(calls).to.include('GRANT CONNECT');
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC branch when SecretString is null', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, null);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('handles partial CDC secret (username only)', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { username: 'partial_user' },
    );
    const result = await handler.handler();
    const queries = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(queries).to.include("rolname='partial_user'");
    expect(result.message).to.equal(expectedMsg);
  });

  it('returns constant message even with CDC and preexisting DB/users', async () => {
    const { handler } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { databaseExists: true, serviceUserExists: true, cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_pass' },
    );
    const result = await handler.handler();
    expect(result).to.deep.equal({ message: expectedMsg });
  });
});
