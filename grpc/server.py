import grpc
from concurrent import futures
import time

# Importamos los archivos autogenerados por Protobuf
import service_pb2
import service_pb2_grpc

class TaskServiceServicer(service_pb2_grpc.TaskServiceServicer):
    
    # 1. Comunicación Unaria
    def ExecuteTask(self, request, context):
        print(f"[Servidor Python] Tarea Unaria recibida: ID {request.task_id} - {request.payload}")
        return service_pb2.TaskResponse(
            task_id=request.task_id,
            status=f"Procesado con éxito vía Unary"
        )

    # 2. Comunicación Bidireccional (Streaming)
    def StreamTasks(self, request_iterator, context):
        print("[Servidor Python] Iniciando stream bidireccional...")
        for request in request_iterator:
            print(f"[Servidor Python] Stream recibido: ID {request.task_id} - {request.payload}")
            time.sleep(1) # Simulamos tiempo de procesamiento de imágenes o datos
            yield service_pb2.TaskResponse(
                task_id=request.task_id,
                status=f"Stream procesado"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_TaskServiceServicer_to_server(TaskServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Servidor gRPC corriendo en puerto 50051...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()