export const ROUTES = {
  HOME: '/',
  LOGIN: '/login',
  SIGNUP: '/signup',
  FORGOT_PASSWORD: '/forgot-password',
  RESET_PASSWORD: '/reset-password',
  PRIVACY: '/privacy',
  TERMS: '/terms',
  DATA_DELETION: '/data-deletion',
  STATUS: '/status',
  APP: '/app',
  APP_DASHBOARD: '/app',
  APP_AUTOMATIONS: '/app/automations',
  APP_DM_AUTOMATION: '/app/dm-automation',
  APP_SETTINGS: '/app/settings',
  APP_BILLING: '/app/billing',
  APP_ADMIN: '/app/admin',
  APP_ADMIN_SPECIFIC_REPLY_DEBUG: '/app/admin/specific-reply-debug',
};

export const APP_CHILD_ROUTES = {
  AUTOMATIONS: 'automations',
  AUTOMATION_DETAIL: 'automations/:id',
  DM_AUTOMATION: 'dm-automation',
  SETTINGS: 'settings',
  BILLING: 'billing',
  ADMIN: 'admin',
  ADMIN_SPECIFIC_REPLY_DEBUG: 'admin/specific-reply-debug',
};
