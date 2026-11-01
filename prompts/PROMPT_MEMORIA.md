# Prompt: memoria de WaterManagement
## Estructura

1. **Introducción y objetivos**, con una tabla de planificación por semanas.
2. **Arquitectura del sistema**: diagrama, tabla de módulos, tecnologías y qué se comunica por sockets y qué por Kafka.
3. **Descripción de los componentes**: WM_Central (con la base de datos y el panel), WM_WS_M, WM_WS_E, WM_FO y wm_common.
4. **Protocolos de comunicación**: formato de trama, mensajes Monitor–Central y Monitor–Engine, topics de Kafka y flujo de un riego.
5. **Tolerancia a fallos**: tabla «Situación | Comportamiento».
6. **Guía de despliegue**: servicios en Railway y comandos Docker para levantar estaciones y operarios.
7. **Explicación del código**: fragmentos clave de cada módulo, cada uno con su explicación.
8. **Pruebas y capturas**: tabla de pruebas con el resultado esperado y las capturas.
9. **Uso de herramientas de inteligencia artificial**.
10. **Conclusiones**: unas 180 palabras.

## Reglas

- Usa solo información del código y del enunciado. Los nombres de clases, funciones, tablas y topics deben coincidir exactamente con el código.
- Los fragmentos de código van copiados tal cual del repositorio.
- No inventes resultados de pruebas. Donde vaya una captura, deja un marcador con su pie de figura.
- Redacción técnica y clara, con frases cortas y tablas para la información comparable.
