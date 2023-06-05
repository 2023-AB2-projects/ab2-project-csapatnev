:: Start MongoDB and wait for it to load
start cmd /K "cd /d ./../MongoDB/mongodb-win32-x86_64-windows-6.0.5/bin && mongod.exe --dbpath ./../../MongoDBFiles"

:: Wait for 3s
timeout /T 2 /NOBREAK

:: Print something to a new console when mongo is up and running
start cmd /K py server.py
start cmd /K py client.py