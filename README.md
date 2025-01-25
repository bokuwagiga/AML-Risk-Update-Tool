
# AML Risk Update Tool

A Python tool for updating AML (Anti-Money Laundering) risk information by processing data from the United Nations Sanctions List. This tool connects to an SQL Server database and inserts relevant data (individuals and entities) into the database tables, while tracking the progress of the process.

## Features
- Fetches the latest AML risk data from the United Nations Consolidated Sanctions List.
- Inserts individuals and entities into SQL Server database.
- Provides real-time progress updates during the data processing.
- User-friendly GUI for inputting database connection credentials.

## Technologies Used
- Python 3.x
- Tkinter (for GUI)
- Requests (for fetching XML data)
- SQLite3 (for database management)
- Logging (for error and progress tracking)

## Requirements
- Python 3.x installed on your machine
- Required Python packages (listed below)

### Install Required Packages

You can install the required Python packages using `pip`. To do this, open your terminal or command prompt and run:

```bash
pip install requests tkinter pyodbc
```

## How to Use
1. Clone the repository or download the project files.
2. Run the `aml_risk_update.py` file using Python.
3. Enter the database connection details (server, database, username, password).
4. Click the **Update AML Risks** button to start processing the data.
5. The tool will display the progress in a progress bar and provide status updates during the process.

### Example Usage

```bash
python aml_risk_update.py
```

Once the tool starts, it will fetch the data from the [UN Sanctions List](https://scsanctions.un.org/resources/xml/en/consolidated.xml) and insert relevant information into the database.

---

### Database Setup

The tool will automatically create the necessary tables in the provided database. You just need to have a functional database connection. The tool interacts with the following tables:

- **ConsolidatedIndividuals**: Stores individual risk data.
- **ConsolidatedEntities**: Stores entity risk data.

Make sure to provide valid database credentials when running the tool, and it will handle the rest.

## Error Handling
If any errors occur during the process, they will be logged and displayed in a message box for the user.

## Logging
All events, including progress updates and errors, are logged for troubleshooting and auditing purposes. Logs are saved to `aml_risk_update.log` in the project directory.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributions
Feel free to fork this repository and submit pull requests. Any improvements or bug fixes are welcome!

## Contact
For any questions or issues, feel free to open an issue on this repository