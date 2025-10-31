import { EntityActionDescriptor } from '@securustablets/libraries.entity-action';
import { EligibilityReason } from '@securustablets/services.inmate.client';
import { TabletRequestState } from './enums/TabletRequestState';

export interface TabletRequest {
  tabletRequestId: number;
  state: TabletRequestState;
  type: TabletRequestType;
  eligibilityReason?: EligibilityReason | null;
  /**
   * @constrain inmateJwt facilityJwt
   */
  customerId: string;
  /**
   * @constrain inmateJwt facilityJwt
   */
  siteId: string;
  locationName: string;
  isActive: boolean;
  /**
   * @constrain inmateJwt
   */
  custodyAccount: string;
  callPartyId: string;
  firstName: string;
  lastName: string;
  ownershipDuration: number; // number in days of the active subscription to their replacement tablet
  tabletCount: number; // number of tablets ordered by the User over the past 2 years
  replacedSerialNumber?: string | null;
  rfid?: string | null;
  serialNumber?: string | null;
  deliveryDate?: string | null;
  deliveredBy?: string | null;
  lastManifestId?: number | null;
  lastManifestDate?: string | null;
  disallowReason?: string | null;
  sortOrder?: number | null;
  debitBalance?: number | null;
  spend?: number | null;
  score?: number | null;
  version: number;
  cdate: string;
  udate: string;
}

export interface TabletRequestAndActions extends TabletRequest {
  actions?: EntityActionDescriptor[];
}

export interface CreateTabletRequest {
  customerId?: string;
  custodyAccount?: string;
  type?: TabletRequestType;
}

export enum TabletRequestType {
  Request = 'request',
  Replacement = 'replacement',
}

export interface TabletRequestCreateParams {
  customerId?: string;
  custodyAccount?: string;
}
