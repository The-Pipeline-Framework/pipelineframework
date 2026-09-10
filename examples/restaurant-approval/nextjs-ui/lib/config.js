export function getUiConfig() {
  return {
    baseUrl: process.env.TPF_BASE_URL || "http://localhost:8081",
    tenantId: process.env.TPF_TENANT_ID || "restaurant-demo",
    awaitStepId: process.env.TPF_AWAIT_STEP_ID || "ProcessAwaitRestaurantDecisionService"
  };
}
