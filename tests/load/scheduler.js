import { check, fail } from 'k6';
import crypto from 'k6/crypto';
import execution from 'k6/execution';
import grpc from 'k6/net/grpc';
import { Rate, Trend } from 'k6/metrics';
import { buildOptions, config } from './config.js';

const client = new grpc.Client();
client.load([__ENV.PROTO_ROOT || '../../src/WorkflowScheduler.Contracts'],
    'Processes/register_process_definition.proto',
    'Processes/start_process.proto',
    'Processes/get_process_status.proto',
    'Processes/send_event.proto',
    'Processes/cancel_process.proto',
    'Steps/complete_step.proto',
    'Steps/is_step_active.proto');

const rpcFailures = new Rate('rpc_failures');
const workloadSuccess = new Rate('workload_success');
const workloadDuration = new Trend('workload_duration', true);
const padding = 'x'.repeat(config.payloadBytes);
let connected = false;
let phase = 'load';

export const options = buildOptions();

function uuid() {
    const bytes = new Uint8Array(crypto.randomBytes(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function connect() {
    if (!connected) {
        client.connect(config.target, { plaintext: config.plaintext, timeout: config.rpcTimeout });
        connected = true;
    }
}

function requireCheck(condition, name) {
    if (!check(condition, { [name]: (value) => value }, { phase })) {
        fail(name);
    }
}

function rpc(method, request) {
    connect();
    const params = { timeout: config.rpcTimeout, tags: { phase, rpc: method } };
    if (__ENV.BEARER_TOKEN) {
        params.metadata = { authorization: `Bearer ${__ENV.BEARER_TOKEN}` };
    }

    let response;
    try {
        response = client.invoke(`workflow.${method}/${method}`, request, params);
    } catch (error) {
        if (phase === 'load') rpcFailures.add(true, { rpc: method, status: 'exception' });
        throw error;
    }
    const success = response.status === grpc.StatusOK;
    if (phase === 'load') rpcFailures.add(!success, { rpc: method, status: String(response.status) });
    requireCheck(success, `${method} returns OK`);
    return response.message;
}

function register(runId, name, steps) {
    const result = rpc('RegisterProcessDefinition', { name: `k6-${runId}-${name}`, version: 1, steps });
    requireCheck(Boolean(result.definitionId), 'Registration returns a definition ID');
    return result.definitionId;
}

function startRequest(definitionId, amount = 500) {
    const key = uuid();
    return {
        definitionId,
        businessKey: `k6:${key}`,
        inputData: JSON.stringify({ orderId: key, amount, padding }),
        idempotencyKey: key,
    };
}

function start(request) {
    const result = rpc('StartProcess', request);
    requireCheck(Boolean(result.processInstanceId), 'Start returns a process ID');
    return result.processInstanceId;
}

function status(processInstanceId) {
    return rpc('GetProcessStatus', {
        processInstanceId, historyOffset: 0, historyPageSize: 10, omitOutputData: true,
    });
}

function sendEvent(processInstanceId, eventName) {
    rpc('SendEvent', { processInstanceId, eventName, correlationId: uuid(), payload: '{"approved":true}' });
}

function wants(workload) {
    return config.workload === 'mixed' || config.workload === workload;
}

export function setup() {
    phase = 'setup';
    try {
        const runId = uuid();
        const data = { definitions: {}, seeds: [] };
        if (wants('workflow')) {
            const high = uuid();
            const low = uuid();
            data.definitions.workflow = register(runId, 'workflow', [
                {
                    id: uuid(), name: 'Choose approval', stepType: 'CONDITION',
                    metadata: JSON.stringify({
                        inputKey: 'amount', operator: 'gte', value: '1000',
                        trueNextStepId: high, falseNextStepId: low,
                    }),
                },
                { id: high, name: 'High approval', stepType: 'WAIT_EVENT', metadata: '{"eventName":"HighApproved"}' },
                { id: low, name: 'Approval', stepType: 'WAIT_EVENT', metadata: '{"eventName":"Approved"}' },
            ]);
        }
        if (wants('read') || wants('replay') || wants('cancel')) {
            data.definitions.wait = register(runId, 'wait', [
                { id: uuid(), name: 'Approval', stepType: 'WAIT_EVENT', metadata: '{"eventName":"Approved"}' },
            ]);
        }
        if (wants('callback')) {
            const wait = uuid();
            data.callStepId = uuid();
            data.definitions.callback = register(runId, 'callback', [
                {
                    id: data.callStepId, name: 'Call', stepType: 'CALL', nextStepId: wait,
                    metadata: JSON.stringify({ serviceName: `k6-${runId}`, command: 'Execute' }),
                },
                { id: wait, name: 'Finish', stepType: 'WAIT_EVENT', metadata: '{"eventName":"CallFinished"}' },
            ]);
        }
        if (wants('read') || wants('replay')) {
            const count = wants('read') ? config.seedCount : 1;
            for (let index = 0; index < count; index++) {
                const request = startRequest(data.definitions.wait);
                const processId = start(request);
                sendEvent(processId, 'Approved');
                requireCheck(status(processId).status === 'Completed', 'Seed process is completed');
                data.seeds.push(processId);
                if (index === 0) data.replay = { request, processId };
            }
        }
        console.log(`Prepared run ${runId}; profile=${config.profile}, workload=${config.workload}.`);
        return data;
    } finally {
        if (connected) client.close();
        connected = false;
        phase = 'load';
    }
}

function workflow(data) {
    // Arrange
    const high = execution.scenario.iterationInTest % 2 === 0;
    const request = startRequest(data.definitions.workflow, high ? 1500 : 500);

    // Act
    const processId = start(request);
    sendEvent(processId, high ? 'HighApproved' : 'Approved');
    const result = status(processId);

    // Assert
    requireCheck(result.status === 'Completed', 'Workflow completes');
    requireCheck(result.totalStepCount === 2 && result.stepHistory.every((step) => step.status === 'Completed'),
        'Condition selects one waiting step');
}

function read(data) {
    // Arrange
    const processId = data.seeds[execution.scenario.iterationInTest % data.seeds.length];

    // Act
    const result = status(processId);

    // Assert
    requireCheck(result.id === processId && result.status === 'Completed', 'Status returns the requested process');
}

function replay(data) {
    // Arrange: all VUs contend on the same persisted start identity.
    const accepted = data.replay;

    // Act
    const processId = start(accepted.request);

    // Assert
    requireCheck(processId === accepted.processId, 'Repeated start returns the original process ID');
}

function callback(data) {
    // Arrange
    const processId = start(startRequest(data.definitions.callback));
    const running = status(processId);
    const step = running.stepHistory.find((candidate) => candidate.id === running.currentStepId);
    requireCheck(Boolean(step) && step.status === 'Running' && step.stepDefinitionId === data.callStepId,
        'Call execution is running');
    const identity = {
        processInstanceId: processId,
        stepDefinitionId: data.callStepId,
        stepInstanceId: step.id,
        attempt: step.retryCount || 0,
    };

    // Act: simulate the Worker callback directly; Kafka delivery latency is not measured.
    requireCheck(rpc('IsStepActive', { ...identity, isCompensation: false }).active === true, 'Call execution is active');
    rpc('CompleteStep', { ...identity, success: true, outputData: '{"ok":true}', correlationId: uuid() });
    sendEvent(processId, 'CallFinished');
    const completed = status(processId);

    // Assert
    requireCheck(completed.status === 'Completed' && completed.totalStepCount === 2, 'Callback advances the workflow');
}

function cancel(data) {
    // Arrange
    const processId = start(startRequest(data.definitions.wait));

    // Act
    rpc('CancelProcess', { processInstanceId: processId });
    const result = status(processId);

    // Assert
    requireCheck(result.status === 'Cancelled' && result.stepHistory[0].status === 'Cancelled', 'Cancellation persists');
}

const workloads = { workflow, read, replay, callback, cancel };
const mixedWorkloads = ['workflow', 'workflow', 'workflow', 'workflow', 'read', 'read', 'read', 'replay', 'callback', 'cancel'];

export default function (data) {
    const workload = config.workload === 'mixed'
        ? mixedWorkloads[execution.scenario.iterationInTest % mixedWorkloads.length]
        : config.workload;
    const started = Date.now();
    let success = false;
    try {
        workloads[workload](data);
        success = true;
    } catch (_) {
        // Failed checks/RPCs are measured; keep offering load instead of aborting the whole run.
        if (connected) client.close();
        connected = false;
    } finally {
        workloadSuccess.add(success, { workload });
        workloadDuration.add(Date.now() - started, { workload });
    }
}
