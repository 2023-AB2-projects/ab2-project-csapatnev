# DBMSName

DBMSName is a command-line database management system (DBMS) implemented in Python.

## Authors

- Gall Vincenzo-David
- Csiki Szabolcs-Attila

## Requirements

DBMSName requires python 3.x. and some additional libraries.
To install all required packages, use `pip`:

```
py -m pip install -r requirements.txt
```

## Installation

After cloning the work repo, you also need to install MongoDB.
Place the installed MongoDB folder inside a wrapper folder "MongoDB", along with another empty folder "MongoDBFiles2"

The following should be the project tree upon correct setup:

```
root
├── ab2-project-csapatnev
├── MongoDB
    ├── MongoDBFiles2
    ├── mongodb-win32-x86_64-windows-6.0.5

```

## Startup

To start, simply run the `start.bat` file located in the main project folder.

## Usage

After the mongoDB and the backend server are both correctly running, you should see the client pop up.
After running `connect`, a confirmation message that a correct connection has been established should be shown.
For further commands and usage, run `commands` inside the client terminal.

## Features

DMBSName proudly features an implemented and working intelliSense, and syntax highlighting for easier view.
You can also run .SQL scripts directly from files, utilizing the `run` command.
