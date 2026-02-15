import {
	feature,
	product,
	featureItem,
	pricedFeatureItem,
	priceItem,
} from "atmn";

// ============================================================
// GPU Metered Features (used in credit schemas)
// ============================================================

export const gpuT4 = feature({
	id: "gpu-t4",
	name: "gpu-t4",
	type: "single_use",
});

export const gpuL4 = feature({
	id: "gpu-l4",
	name: "gpu-l4",
	type: "single_use",
});

export const gpuA10g = feature({
	id: "gpu-a10g",
	name: "gpu-a10g",
	type: "single_use",
});

export const gpuL40s = feature({
	id: "gpu-l40s",
	name: "gpu-l40s",
	type: "single_use",
});

export const gpuA100 = feature({
	id: "gpu-a100",
	name: "gpu-a100",
	type: "single_use",
});

export const gpuA10080gb = feature({
	id: "gpu-a100-80gb",
	name: "gpu-a100-80gb",
	type: "single_use",
});

export const gpuH100 = feature({
	id: "gpu-h100",
	name: "gpu-h100",
	type: "single_use",
});

export const gpuH200 = feature({
	id: "gpu-h200",
	name: "gpu-h200",
	type: "single_use",
});

export const gpuB200 = feature({
	id: "gpu-b200",
	name: "gpu-b200",
	type: "single_use",
});

export const cpu = feature({
	id: "cpu",
	name: "cpu",
	type: "single_use",
});

// ============================================================
// Credit Schema (shared by gpu-credit and gpu-credit-topup)
// 1 credit = $0.01 (1 cent). Costs are credits/second of GPU.
// ============================================================

const GPU_CREDIT_SCHEMA = [
	{ metered_feature_id: gpuT4.id, credit_cost: 0.018 },
	{ metered_feature_id: gpuL4.id, credit_cost: 0.032 },
	{ metered_feature_id: gpuA10g.id, credit_cost: 0.0337 },
	{ metered_feature_id: gpuL40s.id, credit_cost: 0.0596 },
	{ metered_feature_id: gpuA100.id, credit_cost: 0.114 },
	{ metered_feature_id: gpuA10080gb.id, credit_cost: 0.1708 },
	{ metered_feature_id: gpuH100.id, credit_cost: 0.2338 },
	{ metered_feature_id: gpuH200.id, credit_cost: 0.1892 },
	{ metered_feature_id: gpuB200.id, credit_cost: 0.2604 },
	{ metered_feature_id: cpu.id, credit_cost: 0.0042 },
];

// ============================================================
// Credit System Features
// ============================================================

export const gpuCredit = feature({
	id: "gpu-credit",
	name: "GPU Credit (cents)",
	type: "credit_system",
	credit_schema: GPU_CREDIT_SCHEMA,
});

export const gpuCreditTopUp = feature({
	id: "gpu-credit-topup",
	name: "GPU Credit Top Up (cents)",
	type: "credit_system",
	credit_schema: GPU_CREDIT_SCHEMA,
});

// ============================================================
// Limit / Continuous-Use Features
// ============================================================

export const workflowLimit = feature({
	id: "workflow_limit",
	name: "Workflow Limit",
	type: "continuous_use",
});

export const machineLimit = feature({
	id: "machine_limit",
	name: "Machine Limit",
	type: "continuous_use",
});

export const gpuConcurrencyLimit = feature({
	id: "gpu_concurrency_limit",
	name: "GPU Concurrency Limit",
	type: "continuous_use",
});

export const maxAlwaysOnMachine = feature({
	id: "max_always_on_machine",
	name: "Max Always On Machine",
	type: "continuous_use",
});

export const seats = feature({
	id: "seats",
	name: "Seats",
	type: "continuous_use",
});

// ============================================================
// Boolean Features
// ============================================================

export const selfHostedMachines = feature({
	id: "self_hosted_machines",
	name: "Self Hosted Machines",
	type: "boolean",
});

export const customS3 = feature({
	id: "custom_s3",
	name: "Custom S3",
	type: "boolean",
});

// ============================================================
// PRODUCTS
// ============================================================

// ---------- Credit Top-Up (one-time purchase) ----------
export const credit = product({
	id: "credit",
	name: "Credit",
	items: [
		pricedFeatureItem({
			feature_id: gpuCreditTopUp.id,
			price: 0.01,
			included_usage: 0,
			billing_units: 1,
			usage_model: "prepaid",
			reset_usage_when_enabled: false,
		}),
	],
});

// ---------- Free (default plan) ----------
export const free = product({
	id: "free",
	name: "Free",
	is_default: true,
	items: [
		featureItem({
			feature_id: gpuCreditTopUp.id,
			included_usage: 500,
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Creator Monthly ($34/month) ----------
export const creatorMonthly = product({
	id: "creator_monthly",
	name: "Creator (Monthly)",
	items: [
		priceItem({
			price: 34,
			interval: "month",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Creator Yearly ($340/year) ----------
export const creatorYearly = product({
	id: "creator_yearly",
	name: "Creator (Yearly)",
	items: [
		priceItem({
			price: 340,
			interval: "year",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Deployment Monthly ($100/month) ----------
export const deploymentMonthly = product({
	id: "deployment_monthly",
	name: "Deployment (Monthly)",
	items: [
		priceItem({
			price: 100,
			interval: "month",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: seats.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Deployment Yearly ($1000/year) ----------
export const deploymentYearly = product({
	id: "deployment_yearly",
	name: "Deployment (Yearly)",
	items: [
		priceItem({
			price: 1000,
			interval: "year",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: seats.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Business Monthly ($998/month) ----------
export const businessMonthly = product({
	id: "business_monthly",
	name: "Business (Monthly)",
	items: [
		priceItem({
			price: 998,
			interval: "month",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: seats.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});

// ---------- Business Yearly ($9980/year) ----------
export const businessYearly = product({
	id: "business_yearly",
	name: "Business (Yearly)",
	items: [
		priceItem({
			price: 9980,
			interval: "year",
		}),
		pricedFeatureItem({
			feature_id: gpuCredit.id,
			price: 0.01,
			interval: "month",
			included_usage: 999999,
			billing_units: 1,
			usage_model: "pay_per_use",
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: machineLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: workflowLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: gpuConcurrencyLimit.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: maxAlwaysOnMachine.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: seats.id,
			included_usage: 999999,
			reset_usage_when_enabled: false,
		}),
		featureItem({
			feature_id: selfHostedMachines.id,
		}),
		featureItem({
			feature_id: customS3.id,
		}),
	],
});
