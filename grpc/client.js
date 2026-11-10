const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// Cargamos el contrato de forma dinámica
const packageDefinition = protoLoader.loadSync('service.proto', {keepCase: true});
const taskProto = grpc.loadPackageDefinition(packageDefinition).task;

function main() {
    const client = new taskProto.TaskService('localhost:50051', grpc.credentials.createInsecure());

    // 1. Ejecutando llamada Unaria
    console.log("--- Iniciando llamada Unaria ---");
    client.ExecuteTask({ task_id: 1, payload: "Datos iniciales" }, (error, response) => {
        if (!error) {
            console.log(`[Cliente Node] Respuesta unaria recibida: Tarea ${response.task_id} -> ${response.status}`);
        } else {
            console.error(error);
        }

        // 2. Ejecutando Streaming Bidireccional después de la unaria
        console.log("\n--- Iniciando Streaming Bidireccional ---");
        const call = client.StreamTasks();

        // Escuchando respuestas del servidor
        call.on('data', (response) => {
            console.log(`[Cliente Node] Stream respuesta: Tarea ${response.task_id} -> ${response.status}`);
        });

        call.on('end', () => {
            console.log("Stream finalizado por el servidor.");
        });

        // Enviando múltiples mensajes en tiempo real
        let counter = 100;
        const interval = setInterval(() => {
            console.log(`[Cliente Node] Enviando stream: Tarea ${counter}`);
            call.write({ task_id: counter, payload: "Fragmento de datos" });
            counter++;
            if (counter > 103) {
                clearInterval(interval);
                call.end(); // Cerramos la conexión de nuestro lado
            }
        }, 1000);
    });
}

main();