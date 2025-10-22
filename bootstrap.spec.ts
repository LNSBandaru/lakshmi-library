
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

    // --- Secrets mocks
    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'master-test' })
      .resolves({
        SecretString: JSON.stringify({
          username: 'admin_user',
          password: 'admin_password',
        }),
      });

    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'app-test' })
      .resolves({
        SecretString: JSON.stringify({
          username: 'myapp_user',
          password: 'myapp_password',
        }),
      });

    if (env.CDC_USER_SECRET) {
      const payload =
        cdcSecret === null
          ? {}
          : {
              SecretString: JSON.stringify(
                cdcSecret ?? { username: 'cdc_user', password: 'cdc_password' },
              ),
            };
      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((sql) => {
        if (sql.includes('pg_catalog.pg_database'))
          return { rows: [{ exists: databaseExists }] };
        if (sql.includes(`rolname='myapp_user'`))
          return { rows: [{ exists: serviceUserExists }] };
        if (sql.includes(`rolname='cdc_user'`) || sql.includes(`rolname='partial_user'`))
          return { rows: [{ exists: cdcUserExists }] };
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

    return { handler, mainClientStub, serviceClientStub, cdcClientStub };
  }

  const expectedMsg = "Database 'app_database' usernames are ready for use!";

  // ---------------------------------------------------------
  // Non-CDC execution
  it('executes base non-CDC flow', async () => {
    const { handler } = setUp();
    const result = await handler.handler();
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips all CDC logic when env var is missing', async () => {
    const { handler, cdcClientStub } = setUp({}, {}, undefined);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips DB/user creation if both already exist', async () => {
    const { handler, mainClientStub } = setUp({}, { databaseExists: true, serviceUserExists: true });
    await handler.handler();
    const calls = mainClientStub.query.getCalls().map((c) => c.args[0]);
    expect(calls.filter((c) => c.includes('CREATE')).length).to.equal(0);
  });

  // ---------------------------------------------------------
  // CDC variations
  it('creates CDC user and grants when missing', async () => {
    const { handler, mainClientStub, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: false },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    const result = await handler.handler();
    const mainSQL = mainClientStub.query.getCalls().map((q) => q.args[0]).join(' ');
    expect(mainSQL).to.include('CREATE USER cdc_user');
    const grantSQL = cdcClientStub.query.getCalls().map((q) => q.args[0]).join(' ');
    expect(grantSQL).to.include('GRANT CONNECT');
    expect(grantSQL).to.include('CREATE PUBLICATION');
    expect(result.message).to.equal(expectedMsg);
  });

  it('skips CDC user creation when already exists', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      { cdcUserExists: true },
      { username: 'cdc_user', password: 'cdc_password' },
    );
    const result = await handler.handler();
    const allSQL = mainClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(allSQL).to.include("rolname='cdc_user'");
    expect(allSQL).to.not.include('CREATE USER cdc_user');
    expect(result.message).to.equal(expectedMsg);
  });

  it('handles CDC secret with missing SecretString', async () => {
    const { handler, cdcClientStub } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, null);
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.false;
    expect(result.message).to.equal(expectedMsg);
  });

  it('executes CDC block even when username missing', async () => {
    // bootstrap.ts enters CDC branch if cdcUserSecret exists at all
    const { handler, cdcClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { password: 'only-pass' },
    );
    const result = await handler.handler();
    expect(cdcClientStub.connect.called).to.be.true; // branch hit
    const grants = cdcClientStub.query.getCalls().map((c) => c.args[0]).join(' ');
    expect(grants).to.include('GRANT CONNECT');
    expect(result.message).to.equal(expectedMsg);
  });

  it('handles partial CDC secret (username only)', async () => {
    const { handler, mainClientStub } = setUp(
      { CDC_USER_SECRET: 'cdc-test' },
      {},
      { username: 'partial_user' },
    );
    const result = await handler.handler();
    const queries = mainClientStub.query.getCalls().map((q) => q.args[0]).join(' ');
    expect(queries).to.include("rolname='partial_user'");
    expect(result.message).to.equal(expectedMsg);
  });

  it('verifies CDC full cycle still returns correct message', async () => {
    const { handler } = setUp({ CDC_USER_SECRET: 'cdc-test' }, {}, { username: 'cdc_user' });
    const result = await handler.handler();
    expect(result.message).to.equal(expectedMsg);
  });
});
