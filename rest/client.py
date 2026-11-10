import requests
import time

BASE_URL = 'http://localhost:3000/api/tasks'

def run_client():
    print("--- INICIANDO CLIENTE REST (PYTHON) ---")
    
    # 1. GET: Consultar estado inicial
    print("\n1. Obteniendo tareas actuales (GET)...")
    response = requests.get(BASE_URL)
    print(f"Status: {response.status_code} | Body: {response.json()}")
    time.sleep(2)

    # 2. POST: Crear nueva tarea
    print("\n2. Creando nueva tarea (POST)...")
    new_task_data = {"title": "Grabar video del trabajo"}
    response = requests.post(BASE_URL, json=new_task_data)
    print(f"Status: {response.status_code} (Created) | Body: {response.json()}")
    time.sleep(2)

    # 3. PUT: Actualizar la primera tarea para marcarla como completada
    print("\n3. Actualizando la Tarea 1 (PUT)...")
    update_data = {"completed": True}
    response = requests.put(f"{BASE_URL}/1", json=update_data)
    print(f"Status: {response.status_code} | Body: {response.json()}")
    time.sleep(2)

    # 4. DELETE: Borrar la tarea 1
    print("\n4. Eliminando la Tarea 1 (DELETE)...")
    response = requests.delete(f"{BASE_URL}/1")
    print(f"Status: {response.status_code} (No Content)")
    time.sleep(2)

    # 5. GET: Ver el resultado final
    print("\n5. Obteniendo tareas finales (GET)...")
    response = requests.get(BASE_URL)
    print(f"Status: {response.status_code} | Body: {response.json()}")

if __name__ == '__main__':
    run_client()