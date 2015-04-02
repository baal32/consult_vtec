--DROP SEQUENCE EXPORT_ID_SEQ;

CREATE SEQUENCE EXPORT_ID_SEQ
  START WITH 1
  MAXVALUE 999999999999999999999999999
  MINVALUE 0
  NOCYCLE
  NOCACHE
  NOORDER;

--ALTER TABLE EXPORT_STATUS_LOG
-- DROP PRIMARY KEY CASCADE;

--DROP TABLE EXPORT_STATUS_LOG CASCADE CONSTRAINTS;

CREATE TABLE EXPORT_STATUS_LOG
(
  ROWS_PROCESSED  NUMBER,
  ROWS_TOTAL      NUMBER,
  EXPORT_ID       NUMBER,
  STATUS          VARCHAR2(50 BYTE)             DEFAULT NULL,
  ERROR_MSG       VARCHAR2(500 BYTE),
  EXPORT_NAME     VARCHAR2(500 BYTE),
  EXPORT_START    DATE                          DEFAULT SYSDATE,
  ACTIVITY_DATE   DATE                          DEFAULT SYSDATE,
  USERNAME        VARCHAR2(50 BYTE)             DEFAULT USER,
  COMMENTS			VARCHAR2(4000)
);

ALTER TABLE EXPORT_STATUS_LOG ADD 
  CONSTRAINT EXPORT_LOG_PK
  PRIMARY KEY
  (EXPORT_ID);


CREATE OR REPLACE PACKAGE EXPORT_PROGRESS_PKG
AS
   v_new_exportid           EXPORT_STATUS_LOG.EXPORT_ID%TYPE;
   cINITIALIZING   CONSTANT EXPORT_STATUS_LOG.STATUS%TYPE := 'Initializing';
   cRUNNING        CONSTANT EXPORT_STATUS_LOG.STATUS%TYPE := 'Running';
   cCOMPLETED      CONSTANT EXPORT_STATUS_LOG.STATUS%TYPE := 'Completed';
   cERROR          CONSTANT EXPORT_STATUS_LOG.STATUS%TYPE := 'Error';

   PROCEDURE LOG_PROGRESS (p_rownumber IN PLS_INTEGER);

   PROCEDURE INITIALIZE_EXPORT_LOG (
      p_export_name    IN     VARCHAR2,
      p_row_total      IN     PLS_INTEGER DEFAULT NULL,
      p_comments       IN     VARCHAR2 DEFAULT NULL,
      p_out_exportid      OUT PLS_INTEGER);

   PROCEDURE INITIALIZE_EXPORT_LOG (
      p_export_name   IN VARCHAR2,
      p_row_total     IN PLS_INTEGER DEFAULT NULL,
      p_comments      IN VARCHAR2 DEFAULT NULL);

   PROCEDURE CLOSE_EXPORT_LOG (p_error IN VARCHAR2 DEFAULT NULL);

   PROCEDURE CLEAR_EXPORT_LOG (p_export_name IN VARCHAR2 DEFAULT '%');
END EXPORT_PROGRESS_PKG;
/


CREATE OR REPLACE PACKAGE BODY EXPORT_PROGRESS_PKG
AS
   PROCEDURE LOG_PROGRESS (p_rownumber IN PLS_INTEGER)
   IS
   /*
		LOG_PROGRESS
		Notes: Requires INITIALIZE_EXPORT_LOG to have been run, which creates a row in EXPORT_STATUS_LOG and persists the 
		EXPORT_ID to the package variable.  Instead of having the row number passed it could be set to simply increment the existing row number - currently allows for passing arbitrary number in case row chunking is desired (only update every 1000 rows for instance)
		Parameters: p_rownumber represents the row number processed, possibly retrieved with cursorname%ROWCOUNT
		Returns: N/A
		Exceptions: Will raise a custom exception (-20500) if it cannot find v_new_exportid in the package, which indicates INITIALIZE was not run
   */
      ex_log_not_initialized   EXCEPTION;
   BEGIN
      IF v_new_exportid IS NULL
      THEN
         RAISE ex_log_not_initialized;
      END IF;

      UPDATE EXPORT_STATUS_LOG
         SET ROWS_PROCESSED = p_rownumber,
             ACTIVITY_DATE = SYSDATE,
             STATUS = cRUNNING
       WHERE EXPORT_ID = v_new_exportid;

      COMMIT;
   EXCEPTION
      WHEN ex_log_not_initialized
      THEN
         RAISE_APPLICATION_ERROR (-20500, 'Export log row not initialized');
   END;



   PROCEDURE INITIALIZE_EXPORT_LOG (
      p_export_name    IN     VARCHAR2,
      p_row_total      IN     PLS_INTEGER DEFAULT NULL,
      p_comments       IN     VARCHAR2 DEFAULT NULL,
      p_out_exportid      OUT PLS_INTEGER)
   IS
      /*
		INITIALIZE_EXPORT_LOG
		Notes: Creates the initial row in the EXPORT_STATUS_LOG and sets the package variable v_new_exportid used by LOG_PROGRESS and CLOSE_EXPORT_LOG 
		Parameters: p_export_name is the name of the export
					p_row_total (optional) represents the total row count - this is required to calculate percent complete
					p_comments (optional) - any comments or notes about the run
					
		Returns: 	p_out_exportid - returns the unique export_id value in the EXPORT_STATUS_LOG
		Exceptions: N/A
   */
   BEGIN
      SELECT EXPORT_ID_SEQ.NEXTVAL INTO v_new_exportid FROM DUAL;

      p_out_exportid := v_new_exportid;

      INSERT INTO EXPORT_STATUS_LOG (EXPORT_ID,
                                     EXPORT_NAME,
                                     ROWS_PROCESSED,
                                     ROWS_TOTAL,
                                     STATUS,
                                     COMMENTS)
           VALUES (v_new_exportid,
                   p_export_name,
                   0,
                   p_row_total,
                   cINITIALIZING,
                   p_comments);

      COMMIT;
   END;

   PROCEDURE INITIALIZE_EXPORT_LOG (
      p_export_name   IN VARCHAR2,
      p_row_total     IN PLS_INTEGER DEFAULT NULL,
      p_comments      IN VARCHAR2 DEFAULT NULL)
   IS
       /*
		INITIALIZE_EXPORT_LOG
		Notes: Creates the initial row in the EXPORT_STATUS_LOG and sets the package variable v_new_exportid used by LOG_PROGRESS and CLOSE_EXPORT_LOG 
				Overloaded to allow omission of out variable 
		Parameters: p_export_name is the name of the export
					p_row_total (optional) represents the total row count - this is required to calculate percent complete
					p_comments (optional) - any comments or notes about the run
					
		Returns:  N/A
		Exceptions: N/A
   */
      v_temp_exportid   PLS_INTEGER;
   BEGIN
      INITIALIZE_EXPORT_LOG (p_export_name,
                             p_row_total,
                             p_comments,
                             v_temp_exportid);
   END;


   PROCEDURE CLOSE_EXPORT_LOG (p_error IN VARCHAR2 DEFAULT NULL)
   IS
         /*
		CLOSE_EXPORT_LOG
		Notes: Sets the final status of the export log row - if called without an error, sets the row to 'Completed' - if called with an error message
				sets the row to Error and sets ERROR_MSG to the error message
		Parameters: p_error - the error message, if any
					
		Returns:  N/A
		Exceptions: Will raise a custom exception (-20500) if it cannot find v_new_exportid in the package, which indicates INITIALIZE was not run
   */
      ex_log_not_initialized   EXCEPTION;
      v_status EXPORT_STATUS_LOG.STATUS%TYPE := cCOMPLETED;
   BEGIN
      IF v_new_exportid IS NULL
      THEN
         RAISE ex_log_not_initialized;
      END IF;

    
      IF p_error IS NOT NULL
      THEN
         v_status := cERROR;
      END IF;

      UPDATE EXPORT_STATUS_LOG
         SET STATUS = v_status, ERROR_MSG = p_error
       WHERE EXPORT_ID = v_new_exportid;

      COMMIT;
      -- clear the export id
      v_new_exportid := NULL;
   EXCEPTION
      WHEN ex_log_not_initialized
      THEN
         RAISE_APPLICATION_ERROR (-20500, 'Export log row not initialized');
   END;

   PROCEDURE CLEAR_EXPORT_LOG (p_export_name IN VARCHAR2 DEFAULT '%')
   IS
    /*
		CLEAR_EXPORT_LOG
		Notes: Clears the EXPORT_STATUS_LOG table - if p_export_name is provided, clears all rows matching the export name.  If no export name is provided, clears
				all rows
		Parameters: p_export_name - the name of the export(s) to clear from the table
					
		Returns:  N/A
		Exceptions: N/A
   */
   BEGIN
      DELETE FROM EXPORT_STATUS_LOG
            WHERE EXPORT_NAME LIKE p_export_name;

      COMMIT;
      v_new_exportid := NULL;
   END;
END;
/

CREATE OR REPLACE FORCE VIEW EXPORT_STATUS_LOG_VW
(
   EXPORT_NAME,
   STATUS,
   PCT_COMPLETE,
   RUNTIME_SECONDS,
   COMMENTS,
   ERROR_MSG,
   ROWS_TOTAL,
   ROWS_PROCESSED,
   EXPORT_ID,
   EXPORT_START,
   LAST_ACTIVITY,
   USERNAME
)
AS
   SELECT EXPORT_NAME,
          STATUS,
          ROUND(NVL2 (ROWS_TOTAL, ROWS_PROCESSED / ROWS_TOTAL, NULL) * 100,2)
             AS PCT_COMPLETE,
          ROUND((ACTIVITY_DATE - EXPORT_START) * 24 * 60 * 60,2) AS RUNTIME_SECONDS,
		  COMMENTS,
		  ERROR_MSG,
		  ROWS_TOTAL,
		  ROWS_PROCESSED,
          EXPORT_ID,
          EXPORT_START,
          ACTIVITY_DATE AS LAST_ACTIVITY,
          USERNAME
     FROM EXPORT_STATUS_LOG;
