const express = require('express');
const app = express();
const PORT = 3000;

// Middleware para entender JSON automáticamente
app.use(express.json());

// Simulamos una base de datos en memoria
let tasks = [
    { id: 1, title: "Estudiar Sistemas Distribuidos", completed: false }
];

// 1. GET: Leer todos los recursos
app.get('/api/tasks', (req, res) => {
    console.log("[GET] Solicitud para obtener todas las tareas");
    res.status(200).json(tasks);
});

// 2. POST: Crear un nuevo recurso
app.post('/api/tasks', (req, res) => {
    const newTask = {
        id: tasks.length + 1,
        title: req.body.title,
        completed: false
    };
    tasks.push(newTask);
    console.log(`[POST] Nueva tarea creada: ${newTask.title}`);
    res.status(201).json(newTask); // 201 = Created
});

// 3. PUT: Actualizar un recurso existente
app.put('/api/tasks/:id', (req, res) => {
    const taskId = parseInt(req.params.id);
    const task = tasks.find(t => t.id === taskId);
    
    if (task) {
        task.completed = req.body.completed;
        console.log(`[PUT] Tarea ${taskId} actualizada. Completada: ${task.completed}`);
        res.status(200).json(task);
    } else {
        res.status(404).json({ message: "Tarea no encontrada" });
    }
});

// 4. DELETE: Eliminar un recurso
app.delete('/api/tasks/:id', (req, res) => {
    const taskId = parseInt(req.params.id);
    tasks = tasks.filter(t => t.id !== taskId);
    console.log(`[DELETE] Tarea ${taskId} eliminada`);
    res.status(204).send(); // 204 = No Content (borrado exitoso)
});

app.listen(PORT, () => {
    console.log(`Servidor REST de Node.js corriendo en http://localhost:${PORT}`);
    console.log("Esperando peticiones HTTP...\n");
});