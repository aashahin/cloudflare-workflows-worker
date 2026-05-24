export const EMAIL_EVENTS = {
  RESET_PASSWORD: "email/reset-password",
  NEW_ACCOUNT_CREDENTIALS: "email/new-account-credentials",
  CHANGE_EMAIL_VERIFICATION: "email/change-email-verification",
  VERIFICATION: "email/verification",
  CART_RECOVERY: "email/cart-recovery",
  INVITATION: "email/invitation",
  ENROLLMENT_CONFIRMATION: "email/enrollment-confirmation",
  TRIAL_REMINDER: "email/trial-reminder",
  PAYMENT_RECEIPT: "email/payment-receipt",
  WITHDRAWAL_STATUS: "email/withdrawal-status",
  FAILED_PAYMENT_ALERT: "email/failed-payment-alert",
  REFUND_CONFIRMATION: "email/refund-confirmation",
} as const;

export const NOTIFICATION_EVENTS = {
  CREATE: "notification/create",
  CREATE_FOR_CUSTOMER: "notification/create-for-customer",
  BULK_CREATE: "notification/bulk-create",
} as const;

export const PAYMENT_EVENTS = {
  PROCESS_PAYOUT: "payment/process-payout",
} as const;

export const WHATSAPP_EVENTS = {
  SEND_TEMPLATE: "whatsapp/send-template",
} as const;
