set serveroutput on;

DROP TABLE PR_CRED_AUDIT;
/
DROP TABLE PLAN_OPER_AUDIT;
/
DROP TABLE FACT_OPER_AUDIT;
/
DROP TABLE PLAN_OPER;
/
DROP TABLE FACT_OPER;
/
DROP TABLE PR_CRED;
/
DROP TABLE CLIENT;
/

CREATE TABLE CLIENT    (
                        ID                NUMBER (15),
                        CL_NAME           VARCHAR2 (200)   CONSTRAINT NN_CL_NAME NOT NULL,
                        DATE_BIRTH        DATE,
                     
                        CONSTRAINT PK_CLIENT_ID PRIMARY KEY (ID)
                       );
          
/
CREATE TABLE PR_CRED   (
                        ID                NUMBER (15),
                        NUM_DOG           VARCHAR2 (50)   CONSTRAINT NN_NUM_DOG NOT NULL,
                        SUMMA_DOG         NUMBER (12,2)   CONSTRAINT NN_SUMMA_DOG NOT NULL,
                        DATE_BEGIN        DATE            CONSTRAINT NN_DATE_BEGIN NOT NULL,
                        DATE_END          DATE            CONSTRAINT NN_DATE_END NOT NULL,
                        ID_CLIENT         NUMBER (15),
                        COLLECT_PLAN      NUMBER (15)     CONSTRAINT NN_COLLECT_PLAN NOT NULL,
                        COLLECT_FACT      NUMBER (15)     CONSTRAINT NN_COLLECT_FACT NOT NULL,
                        
                        CONSTRAINT PK_PR_CRED_ID                   PRIMARY KEY (ID),
                        CONSTRAINT UNIQUE_NUM_DOG                  UNIQUE (NUM_DOG),
                        CONSTRAINT MIN_SUMMA_DOG                   CHECK (SUMMA_DOG >= 30000),
                        CONSTRAINT FK_ID_CLIENT                    FOREIGN KEY (ID_CLIENT)
                                   REFERENCES CLIENT (ID),
                        CONSTRAINT UNIQUE_COLLECT_PLAN             UNIQUE (COLLECT_PLAN),
                        CONSTRAINT UNIQUE_COLLECT_FACT             UNIQUE (COLLECT_FACT)           
                       );
                       
/
CREATE INDEX PR_CRED_ID_CLIENT ON PR_CRED (ID_CLIENT);
/
CREATE TABLE PLAN_OPER (
                        COLLECTION_ID     NUMBER (15),
                        P_DATE            DATE            CONSTRAINT NN_P_DATE NOT NULL,
                        P_SUMMA           NUMBER (12,2)   CONSTRAINT NN_P_SUMMA NOT NULL,
                        TYPE_OPER         VARCHAR2 (200),
                       
                        CONSTRAINT MIN_P_SUMMA                     CHECK (P_SUMMA > 0),
                        CONSTRAINT FK_COLLECT_PLAN                 FOREIGN KEY (COLLECTION_ID)
                                   REFERENCES PR_CRED (COLLECT_PLAN)
                       );
                       
/
CREATE INDEX PO_Col ON PLAN_OPER (COLLECTION_ID);
/
CREATE TABLE FACT_OPER (
                        COLLECTION_ID     NUMBER (15),
                        F_DATE            DATE            CONSTRAINT NN_F_DATE NOT NULL,
                        F_SUMMA           NUMBER (12,2)   CONSTRAINT NN_F_SUMMA NOT NULL,
                        TYPE_OPER         VARCHAR2 (200),
                       
                        CONSTRAINT MIN_F_SUMMA                     CHECK (F_SUMMA > 0),
                        CONSTRAINT FK_COLLECT_FACT                 FOREIGN KEY (COLLECTION_ID)
                                   REFERENCES PR_CRED (COLLECT_FACT)
                       );  
/
CREATE INDEX FO_Col ON FACT_OPER (COLLECTION_ID);
/


 --DROP TRIGGER Tr_Ban_Update_PR_CRED;

 --Создание триггера на запрет Update таблицы PR_CRED
 CREATE OR REPLACE TRIGGER Tr_Ban_Update_PR_CRED
 BEFORE UPDATE
 ON PR_CRED 
 FOR EACH ROW
 BEGIN
    RAISE_APPLICATION_ERROR (
      num => -20000,
      msg => 'Нельзя обновить данные таблицы PR_CRED!');
  END;
 
 --DROP TRIGGER Tr_Ban_Update_PLAN_OPER; 
 
 --Создание триггера на запрет Update таблицы PLAN_OPER
 CREATE OR REPLACE TRIGGER Tr_Ban_Update_PLAN_OPER
 BEFORE UPDATE
 ON PLAN_OPER 
 FOR EACH ROW
 BEGIN
    RAISE_APPLICATION_ERROR (
      num => -20000,
      msg => 'Нельзя обновить данные таблицы PLAN_OPER!');
  END;
  
 --DROP TRIGGER Tr_Ban_Update_FACT_OPER; 
  
 --Создание триггера на запрет Update таблицы FACT_OPER
 CREATE OR REPLACE TRIGGER Tr_Ban_Update_FACT_OPER
 BEFORE UPDATE
 ON FACT_OPER 
 FOR EACH ROW
 BEGIN
    RAISE_APPLICATION_ERROR (
      num => -20000,
      msg => 'Нельзя обновить данные таблицы FACT_OPER!');
  END;
 
 --Создании таблицы аудита для операций вставки/удаления в таблицу PR_CRED
 CREATE TABLE PR_CRED_AUDIT (
                             ID             NUMBER (15),
                             NUM_DOG        VARCHAR2 (50),
                             SUMMA_DOG      NUMBER (12,2),
                             DATE_BEGIN     DATE,
                             DATE_END       DATE,
                             ID_CLIENT      NUMBER (15),
                             USER_NAME      VARCHAR2 (50),
                             DATE_OPEN      TIMESTAMP,
                             TYPE_OPERATION VARCHAR2 (20)
                            );
 
 SELECT * FROM PR_CRED_AUDIT;
 
 --DROP TRIGGER Tr_Audit_Inst_PR_CRED;
 
 --Создание триггера на вставку/удаление строки в таблицу PR_CRED и логирование события в таблицу PR_CRED_AUDIT
 CREATE OR REPLACE TRIGGER Tr_Audit_PR_CRED
 AFTER INSERT OR DELETE
 ON PR_CRED 
 FOR EACH ROW
 DECLARE
   V_User_Name      VARCHAR2(50);
   V_Type_Operation VARCHAR2 (20);
 BEGIN   
   -- Найти персону username, осуществляющего INSERT в таблицу
   SELECT USER INTO V_User_Name
   FROM DUAL;   
   -- Вставить строку в таблицу PR_CRED_AUDIT
   CASE 
   WHEN INSERTING THEN
        V_Type_Operation := 'INSERT';
        INSERT INTO PR_CRED_AUDIT (ID, NUM_DOG, SUMMA_DOG, DATE_BEGIN, DATE_END, ID_CLIENT, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:new.ID, :new.NUM_DOG, :new.SUMMA_DOG, :new.DATE_BEGIN, :new.DATE_END, :new.ID_CLIENT, V_User_Name, systimestamp, V_Type_Operation);
   WHEN DELETING THEN
        V_Type_Operation := 'DELETE';
        INSERT INTO PR_CRED_AUDIT (ID, NUM_DOG, SUMMA_DOG, DATE_BEGIN, DATE_END, ID_CLIENT, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:old.ID, :old.NUM_DOG, :old.SUMMA_DOG, :old.DATE_BEGIN, :old.DATE_END, :old.ID_CLIENT, V_User_Name, systimestamp, V_Type_Operation);
   END CASE;
 END;
 
--Создании таблицы аудита для операций вставки/удаления в таблицу PR_CRED
 CREATE TABLE FACT_OPER_AUDIT (
                               COLLECTION_ID  NUMBER (15),
                               F_DATE         DATE,
                               F_SUMMA        NUMBER (12,2),
                               TYPE_OPER      VARCHAR2 (200),
                               USER_NAME      VARCHAR2 (50),
                               DATE_OPEN      TIMESTAMP,
                               TYPE_OPERATION VARCHAR2 (20)
                              );
 
 --DROP TRIGGER Tr_Audit_FACT_OPER;
 
 --Создание триггера на вставку/удаление строки в таблицу PR_CRED и логирование события в таблицу PR_CRED_AUDIT
 CREATE OR REPLACE TRIGGER Tr_Audit_FACT_OPER
 AFTER INSERT OR DELETE
 ON FACT_OPER 
 FOR EACH ROW
 DECLARE
   V_User_Name      VARCHAR2(50);
   V_Type_Operation VARCHAR2 (20);
 BEGIN   
   -- Найти персону username, осуществляющего INSERT в таблицу
   SELECT USER INTO V_User_Name
   FROM DUAL;   
   -- Вставить строку в таблицу PR_CRED_AUDIT
   CASE 
   WHEN INSERTING THEN
        V_Type_Operation := 'INSERT';
        INSERT INTO FACT_OPER_AUDIT (COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:new.COLLECTION_ID, :new.F_DATE, :new.F_SUMMA, :new.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   WHEN DELETING THEN
        V_Type_Operation := 'DELETE';
        INSERT INTO FACT_OPER_AUDIT (COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:old.COLLECTION_ID, :old.F_DATE, :old.F_SUMMA, :old.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   END CASE;
 END; 

 --Создании таблицы аудита для операций вставки/удаления в таблицу PR_CRED
 CREATE TABLE PLAN_OPER_AUDIT (
                               COLLECTION_ID  NUMBER (15),
                               P_DATE         DATE,
                               P_SUMMA        NUMBER (12,2),
                               TYPE_OPER      VARCHAR2 (200),
                               USER_NAME      VARCHAR2 (50),
                               DATE_OPEN      TIMESTAMP,
                               TYPE_OPERATION VARCHAR2 (20)
                              );
 
 --DROP TRIGGER Tr_Audit_PLAN_OPER;
 
 --Создание триггера на вставку/удаление строки в таблицу PR_CRED и логирование события в таблицу PR_CRED_AUDIT
 CREATE OR REPLACE TRIGGER Tr_Audit_PLAN_OPER
 AFTER INSERT OR DELETE
 ON PLAN_OPER 
 FOR EACH ROW
 DECLARE
   V_User_Name      VARCHAR2(50);
   V_Type_Operation VARCHAR2 (20);
 BEGIN   
   -- Найти персону username, осуществляющего INSERT в таблицу
   SELECT USER INTO V_User_Name
   FROM DUAL;   
   -- Вставить строку в таблицу PR_CRED_AUDIT
   CASE 
   WHEN INSERTING THEN
        V_Type_Operation := 'INSERT';
        INSERT INTO PLAN_OPER_AUDIT (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:new.COLLECTION_ID, :new.P_DATE, :new.P_SUMMA, :new.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   WHEN DELETING THEN
        V_Type_Operation := 'DELETE';
        INSERT INTO PLAN_OPER_AUDIT (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:old.COLLECTION_ID, :old.P_DATE, :old.P_SUMMA, :old.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   END CASE;
 END;
 
 SELECT * FROM PR_CRED_AUDIT;
 SELECT * FROM PLAN_OPER_AUDIT;
 SELECT * FROM FACT_OPER_AUDIT;

