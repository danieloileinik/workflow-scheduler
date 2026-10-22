function integer(name, fallback, minimum = 1, maximum = 1000000) {
    const value = Number(__ENV[name] === undefined ? fallback : __ENV[name]);
    if (!Number.isInteger(value) || value < minimum || value > maximum) {
        throw new Error(`${name} must be an integer between ${minimum} and ${maximum}.`);
    }
    return value;
}

function choice(name, fallback, allowed) {
    const value = __ENV[name] || fallback;
    if (!allowed.includes(value)) {
        throw new Error(`${name} must be one of: ${allowed.join(', ')}.`);
    }
    return value;
}

function duration(name, fallback) {
    const value = __ENV[name] || fallback;
    if (!/^(?:\d+(?:\.\d+)?(?:ms|s|m|h))+$/.test(value) || !/[1-9]/.test(value)) {
        throw new Error(`${name} must be a positive duration, for example 30s or 3m.`);
    }
    return value;
}

export const config = {
    profile: choice('PROFILE', 'smoke', ['smoke', 'load', 'stress', 'soak']),
    workload: choice('WORKLOAD', 'mixed', ['mixed', 'workflow', 'read', 'replay', 'callback', 'cancel']),
    target: __ENV.GRPC_TARGET || 'localhost:5125',
    plaintext: choice('PLAINTEXT', 'true', ['true', 'false']) === 'true',
    rpcTimeout: duration('RPC_TIMEOUT', '10s'),
    setupTimeout: duration('SETUP_TIMEOUT', '2m'),
    rate: integer('RATE', 20),
    preAllocatedVUs: integer('PRE_ALLOCATED_VUS', 10),
    maxVUs: integer('MAX_VUS', 100),
    seedCount: integer('SEED_COUNT', 10, 1, 1000),
    payloadBytes: integer('PAYLOAD_BYTES', 0, 0, 900000),
    rpcP95: integer('RPC_P95_MS', 500),
    rpcP99: integer('RPC_P99_MS', 1000),
    workloadP95: integer('WORKLOAD_P95_MS', 3000),
};

if (config.target.includes('://') || config.target.includes('/') || !config.target.trim()) {
    throw new Error('GRPC_TARGET must be host:port, without a URL scheme or path.');
}
if (config.maxVUs < config.preAllocatedVUs) {
    throw new Error('MAX_VUS must be greater than or equal to PRE_ALLOCATED_VUS.');
}
if (config.rpcP99 < config.rpcP95) {
    throw new Error('RPC_P99_MS must be greater than or equal to RPC_P95_MS.');
}

export function buildOptions() {
    let scenario;
    if (config.profile === 'smoke') {
        scenario = { executor: 'shared-iterations', vus: 1, iterations: 10, maxDuration: '2m' };
    } else if (config.profile === 'stress') {
        const ramp = duration('RAMP_DURATION', '30s');
        const hold = duration('STAGE_DURATION', '1m');
        scenario = {
            executor: 'ramping-arrival-rate',
            timeUnit: '1s',
            startRate: 0,
            preAllocatedVUs: config.preAllocatedVUs,
            maxVUs: config.maxVUs,
            stages: [
                { target: config.rate, duration: ramp },
                { target: config.rate, duration: hold },
                { target: config.rate * 3, duration: ramp },
                { target: config.rate * 3, duration: hold },
                { target: config.rate * 5, duration: ramp },
                { target: config.rate * 5, duration: hold },
                { target: 0, duration: ramp },
            ],
        };
    } else {
        scenario = {
            executor: 'constant-arrival-rate',
            rate: config.rate,
            timeUnit: '1s',
            duration: duration('DURATION', config.profile === 'soak' ? '30m' : '3m'),
            preAllocatedVUs: config.preAllocatedVUs,
            maxVUs: config.maxVUs,
        };
    }

    const thresholds = {
        'checks{phase:load}': ['rate>=0.99'],
        rpc_failures: ['rate<0.01'],
        workload_success: ['rate>=0.99'],
        'grpc_req_duration{phase:load}': [`p(95)<${config.rpcP95}`, `p(99)<${config.rpcP99}`],
        workload_duration: [`p(95)<${config.workloadP95}`],
    };
    if (config.profile !== 'smoke') {
        thresholds.dropped_iterations = ['count==0'];
    }

    return {
        scenarios: { scheduler: { ...scenario, gracefulStop: '2m' } },
        setupTimeout: config.setupTimeout,
        thresholds,
        summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
        tags: { profile: config.profile, workload_mode: config.workload },
    };
}
