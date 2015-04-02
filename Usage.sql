/*
Usage:
 - calculate total number of rows to be processed
 - call EXPORT_PROGRESS_PKG.INITIALIZE_EXPORT_LOG passing at least an export name, and optionally the total rows to be processed, any notes or comments, and a placeholder variable to get the export_id generated
 - after each row call EXPORT_PROGRESS_PKG.LOG_PROGRESS passing it the row number of the row being processed 
 - when complete call EXPORT_PROGRESS_PKG.CLOSE_EXPORT_LOG - if an error occurred, pass the error message, otherwise call without parameters
		- closing the log will prevent further logging from modifying the row and will require a new initialize to start a new run
 - EXPORT_PROGRESS_PKG.CLEAR_EXPORT_LOG can be called with an export name to clear all exports with a matching name from the log, or alternately without a parameter to clear all rows
 
 - EXPORT_STATUS_LOG_VW shows PCT_COMPLETE and RUNTIME_SECONDS values as well as status and other columns.  PCT_COMPLETE only works if the total rows to be processed was passed to the INITIALIZE_EXPORT_LOG
 
LOG_PROGRESS and CLOSE_EXPORT_LOG will raise custom 20500 errors if INITIALIZE_EXPORT_LOG has not been run
*/ 
 
CREATE OR REPLACE PACKAGE NG_TEST_EXPORT_PROGRESS_PKG AS
    PROCEDURE TEST_EXPORT;
END NG_TEST_EXPORT_PROGRESS_PKG;
/

CREATE OR REPLACE PACKAGE BODY NG_TEST_EXPORT_PROGRESS_PKG
AS
   PROCEDURE TEST_EXPORT
   IS
      CURSOR get_objects
      IS
         SELECT LEVEL just_a_column
FROM dual
CONNECT BY LEVEL <= 1000000;

      v_rownumber PLS_INTEGER;
      v_exportid PLS_INTEGER;
   BEGIN
      DBMS_OUTPUT.put_line ('Starting export');
      EXPORT_PROGRESS_PKG.INITIALIZE_EXPORT_LOG ('Test Export 3', 1000000,NULL);


      OPEN get_objects;

      LOOP
         FETCH get_objects
         INTO v_rownumber;

         EXIT WHEN get_objects%NOTFOUND;

         EXPORT_PROGRESS_PKG.LOG_PROGRESS (get_objects%ROWCOUNT);
      END LOOP;

      CLOSE get_objects;

      EXPORT_PROGRESS_PKG.CLOSE_EXPORT_LOG ();
   END;
END;
/