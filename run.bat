set /p UserInputPath= Please specify path to your mongod.exe file
start cmd /k py main.py
"%UserInputPath%"
pause
