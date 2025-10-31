import { Audit } from '@securustablets/libraries.audit-history';
import {
  SchemeFactory,
  SecurityContextManager,
} from '@securustablets/libraries.httpsecurity';
import {
  InterfaceValidator,
  SchemaController,
} from '@securustablets/libraries.json-schema';
import { Logger, loggerFactory } from '@securustablets/libraries.logging';
import { Postgres } from '@securustablets/libraries.postgres';
import * as cors from 'cors';
import * as express from 'express';
import * as path from 'path';
import {
  ApiApplication,
  ExpressApiIoc,
  Middleware,
} from 'securus.libraries.expressApi';
import * as swaggerUi from 'swagger-ui-express';
import { Container, Inject, Scope, Singleton } from 'typescript-ioc';
import { ContainerConfig } from 'typescript-ioc/container-config';
import { AppConfig } from './utils/AppConfig';

import { CacheManager, Csi } from '@securustablets/libraries.cache';
import { LocalStore } from '@securustablets/libraries.cache-local';
import { findPkgFileSync } from '@securustablets/libraries.utils';
import './controllers/CustomerManifestController';
import './controllers/HeartbeatController';
import './controllers/TabletRequestController';
import './controllers/TabletRequestFlowController';

@Singleton
export class TabletRequestService {
  @Inject
  private config: AppConfig;

  @Inject
  private interfaceValidator: InterfaceValidator;

  public app: express.Application;
  public api: ApiApplication;

  constructor() {
    this.config.init();
    this.app = express();
    this.api = new ApiApplication(this.app);
    this.configure();
  }

  private configure() {
    TabletRequestService.bindAll();

    this.api
      .use(cors())
      .use(Middleware.requestLogger())
      .use(Middleware.responseLogger())
      .useSecurity(
        SchemeFactory.create({
          apiKey: { keys: this.config.security.apiKey.keys },
          jwt: {
            publicKeyPath: this.config.security.jwt.publicKey,
            inmateJwt: true,
            corpJwt: true,
            facilityJwt: true,
            'facilityJwt:beta': true,
            tabletJwt: true,
          },
        }),
      )
      .useValidator(this.interfaceValidator)
      .error(Middleware.errorHandler());
    Audit.init({ app: this.api });
    SchemaController.init({ app: this.api });
    ExpressApiIoc.init(Container, {
      logger: Logger,
      securityContextManager: SecurityContextManager,
    });

    // eslint-disable-next-line @typescript-eslint/no-var-requires, global-require
    const swaggerDoc = require(
      findPkgFileSync(__filename, ['dist', 'swagger.json']),
    );

    this.app.get('/swagger.json', (req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.send(swaggerDoc);
    });

    this.app.use(
      '/api-docs',
      swaggerUi.serve,
      swaggerUi.setup(swaggerDoc, {
        customSiteTitle: 'Tablet Request Service API Swagger Doc',
      }),
    );

    this.api.registerAll(
      path.join(__dirname, './controllers/**/!(*.d).@(js|ts)'),
    );
  }

  public static bindAll() {
    ContainerConfig.addSource('**/!(*.d).@(js|ts)', __dirname);
    const config = Container.get(AppConfig);
    Container.bind(Logger)
      .provider({
        get: () =>
          loggerFactory({
            applicationName: 'tabletRequestService',
            logLevel: config.log.level,
            console: {
              enable: true,
              config: {
                format: config.log.format,
                colorize: config.log.colorize,
              },
            },
          }),
      })
      .scope(Scope.Singleton);
    Postgres.init({
      logger: Container.get(Logger),
      config,
    });
    Container.bind(Postgres).provider({ get: () => Postgres.getInstance() });

    const cacheManager = Container.get(CacheManager);
    cacheManager.init(Container.get(Logger));
    cacheManager.addStore({
      cacheStoreIdentifier: Csi.Local,
      cacheStore: new LocalStore(),
      cacheStoreConfig: config.cache,
    });
  }
}
