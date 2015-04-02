/* Formatted on 3/26/2015 10:00:52 PM (QP5 v5.163.1008.3004) */
CREATE OR REPLACE PACKAGE BODY VS_COOK_TEST.EXPORT_PROGRESS_PKG
AS
   PROCEDURE LOG_PROGRESS (p_rownumber IN PLS_INTEGER)
   IS
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
       WHERE EXPORT_ID = v_new_exportid AND EXPORT_NAME = v_export_name;

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
   BEGIN
      SELECT EXPORT_ID_SEQ.NEXTVAL INTO v_new_exportid FROM DUAL;

      v_export_name := p_export_name;
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
      v_temp_exportid   PLS_INTEGER;
   BEGIN
      INITIALIZE_EXPORT_LOG (p_export_name,
                             p_row_total,
                             p_comments,
                             v_temp_exportid);
   END;


   PROCEDURE CLOSE_EXPORT_LOG (p_error IN VARCHAR2 DEFAULT NULL)
   IS
      ex_log_not_initialized   EXCEPTION;
      v_status VARCHAR2(100) := cCOMPLETED;
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
       WHERE EXPORT_NAME = v_export_name AND EXPORT_ID = v_new_exportid;

      COMMIT;
      -- clear the export id
      v_new_exportid := NULL;
      v_export_name := NULL;
   EXCEPTION
      WHEN ex_log_not_initialized
      THEN
         RAISE_APPLICATION_ERROR (-20500, 'Export log row not initialized');
   END;

   PROCEDURE CLEAR_EXPORT_LOG (p_export_name IN VARCHAR2 DEFAULT '%')
   IS
   BEGIN
      DELETE FROM EXPORT_STATUS_LOG
            WHERE EXPORT_NAME LIKE p_export_name;

      COMMIT;
      v_new_exportid := NULL;
      v_export_name := NULL;
   END;
END;
/