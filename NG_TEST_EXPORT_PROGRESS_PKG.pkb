CREATE OR REPLACE PACKAGE BODY VS_COOK_TEST.NG_TEST_EXPORT_PROGRESS_PKG
AS
   PROCEDURE TEST_EXPORT
   IS
      CURSOR get_objects
      IS
         SELECT owner,
                object_name,
                created,
                last_ddl_time
           FROM ALL_OBJECTS
          WHERE ROWNUM < 20000;

      v_owner     VARCHAR2 (100);
      v_created   DATE;
      v_moddate   DATE;
      v_name      VARCHAR2 (100);
      v_exportid PLS_INTEGER;
   BEGIN
      DBMS_OUTPUT.put_line ('Starting export');
      EXPORT_PROGRESS_PKG.INITIALIZE_EXPORT_LOG ('Test Export', 19999,'Comments',v_exportid);


      OPEN get_objects;

      LOOP
         FETCH get_objects
         INTO v_owner, v_name, v_created, v_moddate;

         EXIT WHEN get_objects%NOTFOUND;

         EXPORT_PROGRESS_PKG.LOG_PROGRESS (get_objects%ROWCOUNT);
      END LOOP;

      CLOSE get_objects;

      EXPORT_PROGRESS_PKG.CLOSE_EXPORT_LOG ();
   END;
END;
/