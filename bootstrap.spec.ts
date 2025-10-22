
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
    existenceOverrides: {
      databaseExists?: boolean;
      serviceUserExists?: boolean;
      cdcUserExists?: boolean;
    } = {},
    cdcSecret:
      | { username?: string; password?: string }
      | null
      | undefined = undefined,
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

    const appDatabase = env.APP_DATABASE_NAME ?? 'myapp';

    // Existence flags (default false to trigger CREATE paths)
    const {
      databaseExists = false,
      serviceUserExists = false,
      cdcUserExists = false,
    } = existenceOverrides;

    // Secrets
    secretsMock
      .on(GetSecretValueCommand, { SecretId: 'master-test' })
      .resolves({
        SecretString: JSON.stringify({
          username: 'admin_user',
          password: 'admin_password',
        }),
      });

    secretsMock.on(GetSecretValueCommand, { SecretId: 'app-test' }).resolves({
      SecretString: JSON.stringify({
        username: 'myapp_user',
        password: 'myapp_password',
      }),
    });

    if (env.CDC_USER_SECRET) {
      let payload: any;

      if (cdcSecret === null) {
        payload = {}; // SecretString missing
      } else if (cdcSecret && (cdcSecret.username || cdcSecret.password)) {
        payload = { SecretString: JSON.stringify(cdcSecret) };
      } else {
        payload = {
          SecretString: JSON.stringify({
            username: 'cdc_user',
            password: 'cdc_password',
          }),
        };
      }

      secretsMock
        .on(GetSecretValueCommand, { SecretId: env.CDC_USER_SECRET })
        .resolves(payload);
    }

    const mainClientStub = {
      database: 'postgres',
      connect: sinon.stub().returnsThis(),
      end: sinon.stub().resolves(),
      query: sinon.stub().callsFake((statement) => {
        if (statement.includes('pg_catalog.pg_database')) {
          return { rows: [{ exists: databaseExists }] };
        }
        if (statement.includes('FROM pg_roles')) {
          if (statement.includes(`'myapp_user'`)) {
            return { rows: [{ exists: serviceUserExists }] };
          }
          if (statement.includes(`'cdc_user'`)) {
            return { rows: [{ exists: cdcUserExists }] };
          }
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

    let ctorCount = 0;
    const pgStub = sinon.stub().callsFake((options) => {
      if (!options.database) {
        ctorCount++;
        return mainClientStub;
      }
      if (options.database === appDatabase) {
        ctorCount++;
        if (ctorCount === 2) return serviceClientStub;
        if (ctorCount === 3 && env.CDC_USER_SECRET) return cdcClientStub;
        return serviceClientStub;
      }
      throw new Error('Unexpected Client instantiation');
    });

    const handler = proxyquire('../../src/bootstrap', {
      pg: { Client: pgStub },
      envalid: { cleanEnv: () => env },
    });

    const consoleSpy = sinon.spy(console, 'log');
    void serviceClientStub;
    return {
      handler,
      pgStub,
      mainClientStub,
      serviceClientStub,
      cdcClientStub,
      consoleSpy,
    };
  }

  describe('handler', () => {
    it('should complete happy path (no CDC)', async () => {
      const { handler, pgStub, mainClientStub, serviceClientStub, consoleSpy } =
        setUp();

      const result = await handler.handler();

      expect(pgStub.firstCall.args[0]).to.deep.equal({
        user: 'admin_user',
        password: 'admin_password',
        host: 'example',
        port: 5432,
      });

      expect(pgStub.secondCall.args[0]).to.deep.equal({
        database: 'app_database',
        user: 'admin_user',
        password: 'admin_password',
        host: 'example',
        port: 5432,
      });

      // Verify statements against master DB
      expect(mainClientStub.connect.calledOnce).to.equal(true);
      expect(mainClientStub.query.getCall(0).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_catalog.pg_database WHERE lower(datname) = lower('app_database'))`,
      );
      expect(mainClientStub.query.getCall(1).args[0]).to.equal(
        `CREATE DATABASE app_database`,
      );
      expect(mainClientStub.query.getCall(2).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='myapp_user')`,
      );
      expect(mainClientStub.query.getCall(3).args[0]).to.equal(
        `CREATE USER myapp_user WITH ENCRYPTED PASSWORD 'myapp_password'`,
      );
      expect(mainClientStub.end.calledOnce).to.equal(true);

      // Verify statements against service DB
      expect(serviceClientStub.connect.calledOnce).to.equal(true);
      expect(serviceClientStub.query.getCall(0).args[0]).to.equal(
        `CREATE SCHEMA IF NOT EXISTS app_schema`,
      );
      expect(serviceClientStub.query.getCall(1).args[0]).to.equal(
        `CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA app_schema CASCADE`,
      );
      expect(serviceClientStub.query.getCall(2).args[0]).to.equal(
        `CREATE EXTENSION IF NOT EXISTS intarray SCHEMA app_schema CASCADE`,
      );
      expect(serviceClientStub.query.getCall(3).args[0]).to.equal(
        `GRANT CONNECT ON DATABASE app_database TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(4).args[0]).to.equal(
        `GRANT CREATE ON DATABASE app_database TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(5).args[0]).to.equal(
        `CREATE SCHEMA IF NOT EXISTS app_schema`,
      );
      expect(serviceClientStub.query.getCall(6).args[0]).to.equal(
        `REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
      );
      expect(serviceClientStub.query.getCall(7).args[0]).to.equal(
        `REVOKE ALL ON DATABASE app_database FROM PUBLIC`,
      );
      expect(serviceClientStub.query.getCall(8).args[0]).to.equal(
        `GRANT USAGE, CREATE ON SCHEMA app_schema TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(9).args[0]).to.equal(
        `ALTER DEFAULT PRIVILEGES IN SCHEMA app_schema GRANT ALL PRIVILEGES ON TABLES TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(10).args[0]).to.equal(
        `GRANT ALL PRIVILEGES on DATABASE app_database to myapp_user`,
      );
      expect(serviceClientStub.query.getCall(11).args[0]).to.equal(
        `ALTER DATABASE app_database OWNER TO myapp_user`,
      );
      expect(serviceClientStub.end.calledOnce).to.equal(true);

      // Verify logs for each statement.
      expect(consoleSpy.firstCall.args[0]).to.equal(
        `[postgres] SELECT exists(SELECT FROM pg_catalog.pg_database WHERE lower(datname) = lower('app_database'))`,
      );
      expect(consoleSpy.callCount).to.equal(16);

      // Verify lambda result (string matches current bootstrap.ts)
      expect(result).to.deep.equal({
        message: `Database 'app_database' usernames are ready for use!`,
      });
    });

    it('should provide default values for some configs (no CDC)', async () => {
      const { handler, pgStub, mainClientStub, serviceClientStub, consoleSpy } =
        setUp({
          APP_DATABASE_NAME: undefined,
          APP_SCHEMA_NAME: undefined,
        });

      const result = await handler.handler();

      expect(pgStub.firstCall.args[0]).to.deep.equal({
        user: 'admin_user',
        password: 'admin_password',
        host: 'example',
        port: 5432,
      });

      expect(pgStub.secondCall.args[0]).to.deep.equal({
        database: 'myapp',
        user: 'admin_user',
        password: 'admin_password',
        host: 'example',
        port: 5432,
      });

      // Verify statements against master DB
      expect(mainClientStub.connect.calledOnce).to.equal(true);
      expect(mainClientStub.query.getCall(0).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_catalog.pg_database WHERE lower(datname) = lower('myapp'))`,
      );
      expect(mainClientStub.query.getCall(1).args[0]).to.equal(
        `CREATE DATABASE myapp`,
      );
      expect(mainClientStub.query.getCall(2).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='myapp_user')`,
      );
      expect(mainClientStub.query.getCall(3).args[0]).to.equal(
        `CREATE USER myapp_user WITH ENCRYPTED PASSWORD 'myapp_password'`,
      );
      expect(mainClientStub.end.calledOnce).to.equal(true);

      // Verify statements against service DB
      expect(serviceClientStub.connect.calledOnce).to.equal(true);
      expect(serviceClientStub.query.getCall(0).args[0]).to.equal(
        `CREATE SCHEMA IF NOT EXISTS myapp_user`,
      );
      expect(serviceClientStub.query.getCall(1).args[0]).to.equal(
        `CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA myapp_user CASCADE`,
      );
      expect(serviceClientStub.query.getCall(2).args[0]).to.equal(
        `CREATE EXTENSION IF NOT EXISTS intarray SCHEMA myapp_user CASCADE`,
      );
      expect(serviceClientStub.query.getCall(3).args[0]).to.equal(
        `GRANT CONNECT ON DATABASE myapp TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(4).args[0]).to.equal(
        `GRANT CREATE ON DATABASE myapp TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(5).args[0]).to.equal(
        `CREATE SCHEMA IF NOT EXISTS myapp_user`,
      );
      expect(serviceClientStub.query.getCall(6).args[0]).to.equal(
        `REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
      );
      expect(serviceClientStub.query.getCall(7).args[0]).to.equal(
        `REVOKE ALL ON DATABASE myapp FROM PUBLIC`,
      );
      expect(serviceClientStub.query.getCall(8).args[0]).to.equal(
        `GRANT USAGE, CREATE ON SCHEMA myapp_user TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(9).args[0]).to.equal(
        `ALTER DEFAULT PRIVILEGES IN SCHEMA myapp_user GRANT ALL PRIVILEGES ON TABLES TO myapp_user`,
      );
      expect(serviceClientStub.query.getCall(10).args[0]).to.equal(
        `GRANT ALL PRIVILEGES on DATABASE myapp to myapp_user`,
      );
      expect(serviceClientStub.query.getCall(11).args[0]).to.equal(
        `ALTER DATABASE myapp OWNER TO myapp_user`,
      );
      expect(serviceClientStub.end.calledOnce).to.equal(true);

      // Verify logs for each statement.
      expect(consoleSpy.firstCall.args[0]).to.equal(
        `[postgres] SELECT exists(SELECT FROM pg_catalog.pg_database WHERE lower(datname) = lower('myapp'))`,
      );
      expect(consoleSpy.callCount).to.equal(16);

      // Verify lambda result.
      expect(result).to.deep.equal({
        message: "Database 'myapp' usernames are ready for use!",
      });
    });

    it('should handle database and user already exist (no CDC)', async () => {
      const { handler, mainClientStub } = setUp(
        {},
        { databaseExists: true, serviceUserExists: true },
      );

      await handler.handler();

      // Verify "EXISTS" statements execute but "CREATE" statements do not.
      expect(mainClientStub.query.getCall(0).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_catalog.pg_database WHERE lower(datname) = lower('app_database'))`,
      );
      expect(mainClientStub.query.getCall(1).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='myapp_user')`,
      );
      expect(mainClientStub.query.callCount).to.equal(2);
    });

    // --- CDC tests added below without changing the existing approach ---

    it('should create CDC user when missing and apply CDC grants/publication', async () => {
      const { handler, pgStub, mainClientStub, cdcClientStub, consoleSpy } =
        setUp({ CDC_USER_SECRET: 'cdc-test' }, { cdcUserExists: false });

      const result = await handler.handler();

      // Third client for CDC db session
      expect(pgStub.thirdCall.args[0]).to.deep.include({
        database: 'app_database',
      });

      // main DB includes CDC role check + creation
      expect(mainClientStub.query.getCall(4).args[0]).to.equal(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`,
      );

      // CDC DB grants/publication
      expect(cdcClientStub.connect.calledOnce).to.equal(true);
      expect(cdcClientStub.query.getCall(0).args[0]).to.equal(
        `GRANT CONNECT ON DATABASE app_database TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(1).args[0]).to.equal(
        `GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(2).args[0]).to.equal(
        `GRANT rds_replication, rds_superuser TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(3).args[0]).to.equal(
        `CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES`,
      );
      expect(cdcClientStub.end.calledOnce).to.equal(true);

      // log count adds 6 more (2 main for cdc existence + 4 cdc grants)
      expect(consoleSpy.callCount).to.equal(22);

      expect(result).to.deep.equal({
        message: `Database 'app_database' usernames are ready for use!`,
      });
    });

    it('should skip CDC user creation if already exists but still apply grants', async () => {
      const { handler, mainClientStub, cdcClientStub, consoleSpy } = setUp(
        { CDC_USER_SECRET: 'cdc-test' },
        { cdcUserExists: true },
      );

      await handler.handler();

      const texts = mainClientStub.query.getCalls().map((c) => c.args[0]);
      expect(texts).to.include(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`,
      );
      expect(texts).to.not.include(
        `CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'cdc_password'`,
      );

      expect(cdcClientStub.query.getCall(0).args[0]).to.equal(
        `GRANT CONNECT ON DATABASE app_database TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(1).args[0]).to.equal(
        `GRANT SELECT ON ALL TABLES IN SCHEMA app_schema TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(2).args[0]).to.equal(
        `GRANT rds_replication, rds_superuser TO cdc_user`,
      );
      expect(cdcClientStub.query.getCall(3).args[0]).to.equal(
        `CREATE PUBLICATION IF NOT EXISTS cdc_publication FOR ALL TABLES`,
      );

      // one fewer log than creation path
      expect(consoleSpy.callCount).to.equal(21);
    });

    it('skips CDC flow when CDC secret SecretString is undefined', async () => {
      const { handler, pgStub, mainClientStub, cdcClientStub } = setUp(
        { CDC_USER_SECRET: 'cdc-test' },
        {},
        null,
      ); // SecretString missing -> cdcUserSecret undefined
      await handler.handler();
      // Guard must short-circuit; no CDC role check/creation
      const mainTexts = mainClientStub.query.getCalls().map((c) => c.args[0]);
      expect(mainTexts).to.not.include(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`,
      );
      expect(mainTexts).to.not.include(
        `CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'cdc_password'`,
      );
      // No CDC client/grants
      expect(pgStub.thirdCall).to.be.null;
      expect(cdcClientStub.connect.called).to.equal(false);
    });

    // SecretString missing -> cdcUserSecret undefined
    it('skips CDC flow when CDC secret SecretString is undefined', async () => {
      const { handler, pgStub, mainClientStub, cdcClientStub } = setUp(
        { CDC_USER_SECRET: 'cdc-test' },
        {},
        null,
      );

      const result = await handler.handler();

      const mainTexts = mainClientStub.query.getCalls().map((c) => c.args[0]);
      expect(mainTexts).to.not.include(
        `SELECT exists(SELECT FROM pg_roles WHERE rolname='cdc_user')`,
      );

      // No third pg client for CDC grants
      expect(pgStub.thirdCall).to.be.null;
      expect(cdcClientStub.connect.called).to.equal(false);

      // Assert non-CDC message to kill the `&& → ||` mutant
      expect(result).to.deep.equal({
        message: `Database 'app_database' usernames are ready for use!`,
      });
    });

    it('skips CDC message logic when username is missing', async () => {
      const { handler } = setUp(
        { CDC_USER_SECRET: 'cdc-test' },
        {},
        { username: 'someuser', password: 'somepass' },
      );

      const result = await handler.handler();

      // Should NOT return the double-username message
      expect(result).to.deep.equal({
        message: `Database 'app_database' usernames are ready for use!`,
      });
    });
  });
});
