// src/core/domain.js
// Domain entity definitions with complete field preservation

const PRODUCTION_STATUS = {
  ON_PRODUCTION: 'On Production',
  TESTING: 'Testing',
  STANDBY: 'Standby',
  LOCKED_POTENTIAL: 'Locked Potential',
  SHUT_IN: 'Shut-in',
  MOTHBALL: 'Mothball'
};

const WORKFLOW_PHASES = {
  FIELD_REVIEW: 'Field Review',
  PACKAGE_PREP: 'Package Preparation',
  RFI: 'RFI',
  DEPRESSURIZING: 'Depressurizing',
  EXECUTION: 'Execution',
  CLOSED: 'Closed'
};

const DN_TYPES = {
  SAND_ENCROACHMENT: { name: 'Sand Encroachment', severity: 'High', impact: 'Production' },
  PINHOLE_LEAK: { name: 'Pinhole Leak', severity: 'Medium', impact: 'Safety' },
  INSTRUMENT_FAILURE: { name: 'Instrument Failure', severity: 'Medium', impact: 'Data' },
  VALVE_FAILURE: { name: 'Valve Failure', severity: 'High', impact: 'Flow' },
  FLOWLINE_CORROSION: { name: 'Flowline Corrosion', severity: 'Low-Medium', impact: 'Progressive' },
  TUBING_ISSUE: { name: 'Tubing Issue', severity: 'Medium', impact: 'Variable' }
};

class Well {
  constructor(data) {
    this.well_id = data.well_id || data.id || '';
    this.id = this.well_id;
    this.well_name = String(data.well_name || data.name || '').trim();
    this.name = this.well_name;
    this.field = data.field || '';
    this.production_status = data.production_status || 'Unknown';
    this.oil_rate_bopd = Number(data.oil_rate_bopd || 0);
    this.oil_rate = this.oil_rate_bopd;
    this.last_updated = data.last_updated || '';
    this.field_code = data.field_code || this._extractFieldCode();
    this.is_active = String(this.production_status || '').toLowerCase() !== 'shut-in';
    this.metadata = { ...data };
  }

  _extractFieldCode() {
    const name = String(this.well_name || '').toUpperCase().trim();
    if (name.startsWith('ANDR-')) return 'ANDR';
    if (name.startsWith('ABQQ-')) return 'ABQQ';
    const fieldName = String(this.field || '').toLowerCase().trim();
    if (fieldName === 'ain dar') return 'ANDR';
    if (fieldName === 'abqaiq') return 'ABQQ';
    return '';
  }

  isProducing() {
    const status = String(this.production_status || '').toLowerCase();
    return status === 'on production' || status === 'testing';
  }

  isActive() {
    const status = String(this.production_status || '').toLowerCase();
    return status !== 'shut-in' && status !== 'shut in';
  }

  isPotentiallyLocked() {
    const status = String(this.production_status || '').toLowerCase();
    return status.includes('locked');
  }

  getFieldCode() {
    return this.field_code;
  }

  getBaselineRate() {
    return this.oil_rate_bopd;
  }

  toPlainObject() {
    return {
      well_id: this.well_id,
      id: this.well_id,
      well_name: this.well_name,
      name: this.well_name,
      field: this.field,
      production_status: this.production_status,
      oil_rate_bopd: this.oil_rate_bopd,
      oil_rate: this.oil_rate_bopd,
      last_updated: this.last_updated,
      field_code: this.field_code,
      is_active: this.is_active,
      ...this.metadata
    };
  }
}

class DN {
  constructor(data) {
    this.dn_id = data.dn_id || '';
    this.id = this.dn_id;
    this.well_id = data.well_id || '';
    this.dn_type = data.dn_type || '';
    this.type = this.dn_type;
    this.dn_type_id = data.dn_type_id || '';
    this.type_group = data.type_group || '';
    this.priority = data.priority || 'Unknown';
    this.created_date = data.created_date || '';
    this.progress_percent = Number(data.progress_percent) || 0;
    this.dn_status = data.dn_status || data.status_update || '';
    this.status_update = this.dn_status;
    this.dn_owner = data.dn_owner || data.updated_by || '';
    this.owner = this.dn_owner;
    this.current_owner_name = this.dn_owner;
    this.update_date = data.update_date || '';
    this.workflow_status = this._interpretWorkflowStatus();
    this.current_step = data.current_step || this._interpretCurrentStep();
    this.is_closed = this.workflow_status === 'Closed';
    this.metadata = { ...data };
  }

  _interpretWorkflowStatus(statusText = this.dn_status) {
    const s = String(statusText || '').toLowerCase().trim();
    if (!s) return 'Open';
    if (s.includes('closed')) return 'Closed';
    if (s.includes('completed')) return 'Completed';
    if (s.includes('not issuing')) return 'Waiting';
    if (s.includes('under rfi')) return 'In Progress';
    if (s.includes('depressurizing')) return 'In Progress';
    return 'In Progress';
  }

  _interpretCurrentStep(statusText = this.dn_status) {
    const s = String(statusText || '').toLowerCase().trim();
    if (s.includes('not issuing package')) return 'Package Preparation';
    if (s.includes('dn not issuing')) return 'Package Preparation';
    if (s.includes('not issuing')) return 'Package Preparation';
    if (s.includes('under rfi')) return 'RFI';
    if (s.includes('depressurizing')) return 'Execution';
    if (s.includes('completed')) return 'Execution';
    return 'Field Review';
  }

  isClosed() {
    const status = String(this.dn_status || '').toLowerCase();
    return status.includes('closed');
  }

  isActive() {
    return !this.isClosed();
  }

  getPhase() {
    const status = String(this.dn_status || '').toLowerCase();
    if (status.includes('closed') || status.includes('completed')) return WORKFLOW_PHASES.CLOSED;
    if (status.includes('depressurizing')) return WORKFLOW_PHASES.DEPRESSURIZING;
    if (status.includes('execution') || status.includes('40%') || status.includes('60%')) return WORKFLOW_PHASES.EXECUTION;
    if (status.includes('rfi') || status.includes('under rfi')) return WORKFLOW_PHASES.RFI;
    if (status.includes('not issuing') || status.includes('package')) return WORKFLOW_PHASES.PACKAGE_PREP;
    return WORKFLOW_PHASES.FIELD_REVIEW;
  }

  getDaysSinceCreation() {
    if (!this.created_date) return 0;
    try {
      const created = new Date(this.created_date);
      const now = new Date('2026-04-02');
      const diffTime = Math.abs(now - created);
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      return diffDays;
    } catch (e) {
      return 0;
    }
  }

  getDaysSinceLastUpdate() {
    if (!this.update_date) return 0;
    try {
      const updated = new Date(this.update_date);
      const now = new Date('2026-04-02');
      const diffTime = Math.abs(now - updated);
      const diffDays = Math.ceil(diffTime / (1000 * 60 * 60 * 24));
      return diffDays;
    } catch (e) {
      return 0;
    }
  }

  isDaysStuck(threshold = 14) {
    return this.getDaysSinceLastUpdate() >= threshold && this.isActive();
  }

  toPlainObject() {
    return {
      dn_id: this.dn_id,
      id: this.dn_id,
      well_id: this.well_id,
      dn_type: this.dn_type,
      type: this.dn_type,
      dn_type_id: this.dn_type_id,
      type_group: this.type_group,
      priority: this.priority,
      created_date: this.created_date,
      progress_percent: this.progress_percent,
      dn_status: this.dn_status,
      status_update: this.dn_status,
      dn_owner: this.dn_owner,
      owner: this.dn_owner,
      current_owner_name: this.dn_owner,
      update_date: this.update_date,
      workflow_status: this.workflow_status,
      current_step: this.current_step,
      is_closed: this.is_closed,
      ...this.metadata
    };
  }
}

module.exports = {
  Well,
  DN,
  PRODUCTION_STATUS,
  WORKFLOW_PHASES,
  DN_TYPES
};