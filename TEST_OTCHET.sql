set serveroutput on;

SELECT * 
FROM CLIENT;

SELECT *
FROM PR_CRED;

SELECT * 
FROM PLAN_OPER;

SELECT *
FROM FACT_OPER; 

CREATE OR REPLACE PACKAGE Credit_Portfolio AS
    --������� �������� ����� ����������� ��������� � ���������  ��� ��������� �������� �� �������� ����
    FUNCTION REST_OF_PERCENT (DATE_STATUS IN DATE, Collect_P_id IN FACT_OPER.F_SUMMA%TYPE, Collect_F_id IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE;
    
     --������� �������� ������� ������� ������������� ��� ��������� �������� �� �������� ����
    FUNCTION REST_OF_CREDIT (DATE_STATUS IN DATE, Collect_id_F IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE;
    
    --������� ������� ������� ������������ �������
    FUNCTION Fun_Amount_Annuity_Payment (Sum_Credit IN NUMBER, Percent_Rate IN NUMBER, Month_Count IN NUMBER)
    RETURN NUMBER;

    END Credit_Portfolio;
/
CREATE OR REPLACE PACKAGE BODY Credit_Portfolio AS
    FUNCTION REST_OF_PERCENT (DATE_STATUS IN DATE, Collect_P_id IN FACT_OPER.F_SUMMA%TYPE, Collect_F_id IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE
    IS
    V_Size_Percent      PLAN_OPER.P_SUMMA%TYPE;
    V_Paid_Percent      FACT_OPER.F_SUMMA%TYPE;
    V_Rest_Percent      FACT_OPER.F_SUMMA%TYPE;

    BEGIN 
         SELECT SUM (P_SUMMA) INTO V_Size_Percent
         FROM PLAN_OPER
         WHERE COLLECTION_ID = Collect_P_id AND
               TYPE_OPER = '��������� ���������';
          
         SELECT SUM (F_SUMMA) INTO V_Paid_Percent
         FROM FACT_OPER
         WHERE F_DATE <= DATE_STATUS AND
               COLLECTION_ID = Collect_F_id AND
               TYPE_OPER = '��������� ���������'; 
               
         IF   V_Paid_Percent IS NOT NULL THEN
              V_Rest_Percent :=  V_Size_Percent - V_Paid_Percent;
         ELSE   
              V_Rest_Percent :=  V_Size_Percent;
         END IF;     
         RETURN V_Rest_Percent;
    END REST_OF_PERCENT;

    FUNCTION REST_OF_CREDIT (DATE_STATUS IN DATE, Collect_id_F IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE
    IS
    V_Size_Credit       FACT_OPER.F_SUMMA%TYPE;
    V_Paid_Credit       FACT_OPER.F_SUMMA%TYPE;
    V_Rest_Credit       FACT_OPER.F_SUMMA%TYPE;

    BEGIN 
         SELECT F_SUMMA INTO V_Size_Credit
         FROM FACT_OPER
         WHERE F_DATE < DATE_STATUS AND
               COLLECTION_ID = Collect_id_F AND
               TYPE_OPER = '������ �������';
          
         SELECT SUM (F_SUMMA) INTO V_Paid_Credit
         FROM FACT_OPER
         WHERE F_DATE <= DATE_STATUS AND
               COLLECTION_ID = Collect_id_F AND
               TYPE_OPER = '��������� �������'; 
          
         IF   V_Paid_Credit IS NOT NULL THEN
              V_Rest_Credit :=  V_Size_Credit - V_Paid_Credit;
         ELSE   
              V_Rest_Credit :=  V_Size_Credit;
         END IF;
         RETURN V_Rest_Credit;
    END REST_OF_CREDIT;
    
    FUNCTION Fun_Amount_Annuity_Payment (Sum_Credit IN NUMBER, Percent_Rate IN NUMBER, Month_Count IN NUMBER)
    RETURN NUMBER
    IS
    V_Amount_Annuity_Payment NUMBER;
    V_Percent_Rate NUMBER;
    
    BEGIN
         V_Percent_Rate := Percent_Rate / 12 / 100;
         V_Amount_Annuity_Payment := Sum_Credit * (V_Percent_Rate * POWER((1 + V_Percent_Rate), Month_Count)) / (POWER((1 + V_Percent_Rate), Month_Count) - 1);
         RETURN ROUND (V_Amount_Annuity_Payment, 2);
    END Fun_Amount_Annuity_Payment;
    
END Credit_Portfolio;

--����� ���������� ������ �� ����� SQL ����� ���������� �������

drop FUNCTION Credit_Function;
/
drop type NT_Credit;
/
drop type Type_Credit_Object;



CREATE TYPE Type_Credit_Object AS OBJECT
(
 NUM_DOG        VARCHAR2 (50),
 CL_NAME        VARCHAR2 (200),
 SUMMA_DOG      NUMBER (12,2),
 DATE_BEGIN     DATE,
 DATE_END       DATE,
 REST_CREDIT    NUMBER (12,2),
 REST_PERCENT   NUMBER (12,2),
 DATE_OTCHOT    DATE
);
/
--������� ��������� ���� nested table 
CREATE TYPE NT_Credit AS TABLE OF Type_Credit_Object;
/
CREATE OR REPLACE FUNCTION Credit_Function (Date_Otchot IN DATE)
          RETURN NT_Credit PIPELINED AS
BEGIN
    FOR i IN ( SELECT NUM_DOG, CL_NAME, SUMMA_DOG, DATE_BEGIN, DATE_END, 
                      Credit_Portfolio.REST_OF_CREDIT (Date_Otchot, COLLECT_FACT) REST_CREDIT, 
                      Credit_Portfolio.REST_OF_PERCENT (Date_Otchot, COLLECT_PLAN, COLLECT_FACT) REST_PERCENT, Date_Otchot ����_������  
             FROM PR_CRED PR
             JOIN CLIENT CL
             ON PR.ID_CLIENT = CL.ID
             WHERE DATE_BEGIN < Date_Otchot
             ORDER BY COLLECT_PLAN)
    LOOP
    PIPE ROW (Type_Credit_Object (i.NUM_DOG, i.CL_NAME, i.SUMMA_DOG, i.DATE_BEGIN, i.DATE_END, i.REST_CREDIT, i.REST_PERCENT, i.����_������));    
    END LOOP;
  RETURN;
END;

--����� ������� �� �������� ������������� ���� ��� ��������� ������
SELECT * FROM TABLE (Credit_Function('30-10-2020'));
 
--������������ ������ ������� ���������
--�������� ��������� ������� ��� ���������� ���������� ������
drop table Test_Otchot_1;
CREATE GLOBAL TEMPORARY TABLE Test_Otchot_1 (
                                             NUM_DOG VARCHAR2 (50),
                                             CL_NAME VARCHAR2 (200),
                                             SUMMA_DOG NUMBER (12,2),
                                             DATE_BEGIN DATE,
                                             DATE_END DATE,
                                             REST_CREDIT NUMBER (12,2),
                                             REST_PERCENT NUMBER (12,2),
                                             Otchot_Date DATE
                                            );

 CREATE OR REPLACE PROCEDURE Proc_Test_Otchot (Date_Otchot IN DATE) 
 IS
 --����������� ������ ��� ������ �������� �� ������ � ������� �������
 TYPE Nt_Otchot IS TABLE OF Test_Otchot_1%ROWTYPE;
 Nt_Client_Otchot Nt_Otchot;
 TYPE Nt_Sum_Oper IS TABLE OF FACT_OPER.F_SUMMA%TYPE;
 Nt_All_Credit          Nt_Sum_Oper;
 Nt_Paid_Credit         Nt_Sum_Oper;
 Nt_All_Percent         Nt_Sum_Oper;
 Nt_Paid_Percent        Nt_Sum_Oper;
 V_Rest_Credit          NUMBER (12,2);
 V_Rest_Percent         NUMBER (12,2);
 BEGIN
        COMMIT; -- ��� ������� ��������� ������� Test_Otchot_1
        
        SELECT PC.NUM_DOG ND, CO.CL_NAME CN, PC.SUMMA_DOG SD, PC.DATE_BEGIN DB, PC.DATE_END DE, 0, 0, Date_Otchot BULK COLLECT INTO Nt_Client_Otchot
                                  FROM PR_CRED PC
                                  JOIN CLIENT CO
                                  ON PC.ID_CLIENT = CO.ID
                                  WHERE PC.DATE_BEGIN < Date_Otchot
                                  ORDER BY PC.COLLECT_PLAN;
                          
        IF Nt_Client_Otchot IS EMPTY THEN
        RAISE NO_DATA_FOUND;           -- ����� ����������, ���� �� �������� ���� ��� ������� ���������
        END IF;
        
        --�������� ��������� �������� �������� �� �������� ���� Date_Otchot
        SELECT F_SUMMA BULK COLLECT INTO Nt_All_Credit FROM FACT_OPER
        WHERE F_DATE < Date_Otchot AND
              TYPE_OPER = '������ �������'
        ORDER BY COLLECTION_ID;
        
        --�������� ��������� ���������� ����� ���� �������� �� �������� ���� Date_Otchot  
        SELECT SUMMA BULK COLLECT INTO Nt_Paid_Credit FROM (SELECT COLLECTION_ID, SUM (F_SUMMA) SUMMA 
        FROM FACT_OPER
        WHERE F_DATE <= Date_Otchot AND
              TYPE_OPER = '��������� �������'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID); 
        
        --�������� ��������� ��������� �� ��������   
        SELECT SUMMA BULK COLLECT INTO Nt_All_Percent FROM (SELECT COLLECTION_ID, SUM (P_SUMMA) SUMMA 
        FROM PLAN_OPER
        WHERE TYPE_OPER = '��������� ���������'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID);
          
        --�������� ��������� ���������� ��������� �� �������� �� �������� ���� Date_Otchot 
        SELECT SUMMA BULK COLLECT INTO Nt_Paid_Percent FROM (SELECT COLLECTION_ID, SUM (F_SUMMA) SUMMA 
        FROM FACT_OPER
        WHERE F_DATE <= Date_Otchot AND
              TYPE_OPER = '��������� ���������'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID);

        DBMS_OUTPUT.PUT_LINE (RPAD ('NUM_DOG', 15) || 
                              RPAD ('CL_NAME', 40) || 
                              RPAD ('SUM_DOG', 15) || 
                              RPAD ('DATE_BEGIN', 15) ||
                              RPAD ('DATE_END', 15) || 
                              RPAD ('REST_CREDIT', 15) ||
                              RPAD ('REST_PERCENT', 15) ||
                              RPAD ('REPORT_DT', 15));

        FOR i IN Nt_Client_Otchot.FIRST..Nt_Client_Otchot.LAST 
        LOOP        
        --����������� ������� ������� �������������
        IF i <= Nt_Paid_Credit.LAST THEN
              V_Rest_Credit := Nt_All_Credit (i) - Nt_Paid_Credit (i);
        ELSE  V_Rest_Credit := Nt_All_Credit (i);
        END IF;
        
        --����������� ����� ����������� ���������
        IF i <= Nt_Paid_Percent.LAST THEN
              V_Rest_Percent := Nt_All_Percent (i) - Nt_Paid_Percent (i);
        ELSE  V_Rest_Percent := Nt_All_Percent (i);
        END IF;
        
        --���������� ���������� �� ������� ������� � ��������� � ��������� � ����� �� ����� �������� ��������� (������� �����)
        Nt_Client_Otchot(i).REST_CREDIT := V_Rest_Credit;
        Nt_Client_Otchot(i).REST_PERCENT := V_Rest_Percent;
        
        DBMS_OUTPUT.PUT_LINE (RPAD (Nt_Client_Otchot (i).NUM_DOG, 15) ||
                              RPAD (Nt_Client_Otchot (i).CL_NAME, 40) || 
                              RPAD (Nt_Client_Otchot (i).SUMMA_DOG, 15) ||
                              RPAD (Nt_Client_Otchot (i).DATE_BEGIN, 15) || 
                              RPAD (Nt_Client_Otchot (i).DATE_END, 15) || 
                              RPAD (Nt_Client_Otchot (i).REST_CREDIT, 15) ||
                              RPAD (Nt_Client_Otchot (i).REST_PERCENT, 15) ||
                              RPAD (Nt_Client_Otchot (i).Otchot_Date, 15));
        END LOOP;
        
        --������� ������ �� ��������� ������� Test_Otchot_1 �� �������������� ��������� 
        FORALL j IN Nt_Client_Otchot.FIRST..Nt_Client_Otchot.LAST
        INSERT INTO Test_Otchot_1 VALUES Nt_Client_Otchot (j);
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE ('������! � ������� ��� �������� �������� �� �������� ���� ������ '||Date_Otchot);
        GOTO end_of_proc;
        RAISE;
        <<end_of_proc>>
  NULL;
 END;

BEGIN
Proc_Test_Otchot ('30-11-2020');
END;

SELECT * FROM Test_Otchot_1;
 


 --�������� ��������� "O������� ���������� ��������"
 CREATE OR REPLACE PROCEDURE OPEN_DOGOVOR (Number_Dog    IN PR_CRED.NUM_DOG%TYPE,
                                           Sum_Dog       IN VARCHAR2,
                                           Date_Open     IN PR_CRED.DATE_BEGIN%TYPE,
                                           Month_Count   IN VARCHAR2)
 IS
 --������ ���������� ��� ���������������� ����������
 V_Sum_Dog          NUMBER (10);
 V_Month_Count      NUMBER (3);
 Min_Date           DATE;
 TYPE Nt_of_NUM_DOG IS TABLE OF PR_CRED.NUM_DOG%TYPE;
 Nt_of_Exception    Nt_of_NUM_DOG;
 Num_Dog_Exc        EXCEPTION;
 Unique_Constraint  EXCEPTION;
 Range_Summa_Exc    EXCEPTION;
 Min_Date_Exc       EXCEPTION;
 Range_Month_Exc    EXCEPTION;
 
 --������ ���������� ��� ������� ������ �������� ������ �������� � ������� PR_CRED
 V_ID               PR_CRED.ID%TYPE;
 V_Id_Client        PR_CRED.ID_CLIENT%TYPE;
 V_Collect_Plan     PR_CRED.COLLECT_PLAN%TYPE;
 V_Collect_Fact     PR_CRED.COLLECT_FACT%TYPE;
 Add_Rec_Open_Dog   PR_CRED%ROWTYPE;
 Info_Opened_Dog    PR_CRED%ROWTYPE;
 
  --������ ���������� ��� ������� ������� �������� �������� ��� ������ �������� � ������� PLAN_OPER
 V_P_Date           PLAN_OPER.P_DATE%TYPE;
 V_Percent_Credit   NUMBER;--PLAN_OPER.P_SUMMA%TYPE;
 V_Paid_Credit      NUMBER (12,2) := 0;
 V_Body_Credit      NUMBER (12,2) := 0;
 V_Amount_Payment   NUMBER;
 V_Percent_Rate     NUMBER;
 Add_Rec_Plan_Oper  PLAN_OPER%ROWTYPE;
 Info_Add_Rec_Plan  PLAN_OPER%ROWTYPE;
 
 BEGIN
       --�������� ��������� ���������� ��������� Sum_Dog � Month_Count �� ������������ ������������� ��������� ���� ������ � ����� ���������� � ������ ��������������
       V_Sum_Dog := TRUNC (TO_NUMBER (Sum_Dog));
       V_Month_Count := TRUNC (TO_NUMBER (Month_Count));
       
       --����� ���������� �� ������ ��������� ������ �������� ������-������ ������� ��������������� �������� ���������
       
       --����������� ���� �������� ���������� ��������
       SELECT MAX (DATE_BEGIN) INTO Min_Date FROM PR_CRED;
       
       --�������� ��������� ������������ � ������� ���������
       SELECT NUM_DOG BULK COLLECT INTO Nt_of_Exception FROM PR_CRED ORDER BY DATE_BEGIN;
       
       CASE
       WHEN REGEXP_SUBSTR (Number_Dog, '2021/\d+') IS NULL THEN
       RAISE Num_Dog_Exc;                         -- ����� ���������� ��� ������������ ����� ������ ��������
       
       WHEN Number_Dog MEMBER OF Nt_of_Exception THEN
       RAISE Unique_Constraint;                   -- ����� ���������� ��� ������� � ������� ��������� ��������
       
       WHEN V_Sum_Dog  <  30000 OR V_Sum_Dog > 5000000 THEN
       RAISE Range_Summa_Exc;                     -- ����� ���������� ��� ������ �� ������� ���������� �������� ����� ������� ��� ��������� ��������

       WHEN Date_Open  < Min_Date THEN
       RAISE Min_Date_Exc;                        -- ����� ����������, ���� ��������� ���� ������ ���� ���������� ��������� �������� 
       
       WHEN V_Month_Count  <  3 OR V_Month_Count > 60 THEN
       RAISE Range_Month_Exc;                     -- ����� ���������� ��� ������ �� ������� ���������� �������� ����� ������� ��� ��������� ��������
       ELSE NULL;
       END CASE;
       
        --���������� ������ �������� ��������������
       SELECT MAX (ID) + 1111 INTO V_ID FROM PR_CRED;
        
       --����� ������� � ���������� ���������� ������ ��������� ���������, �������� ����������� �������
       SELECT ID_CLIENT INTO V_Id_Client 
       FROM (SELECT ID_CLIENT, SUM (SUMMA_DOG) SUM_ALL_DOG 
             FROM PR_CRED 
             GROUP BY ID_CLIENT 
            ORDER BY SUM_ALL_DOG)
       WHERE rownum = 1;
       
        --���������� ������ �������� �������������� �������� ��������
       SELECT MAX (COLLECT_PLAN) + 111 INTO V_Collect_Plan FROM PR_CRED;
        
       --���������� ������ �������� �������������� ����������� ��������
       V_Collect_Fact := V_Collect_Plan - 3;
       
       --���������� ������ Add_Rec_Open_Dog ������������ ����������
       Add_Rec_Open_Dog.ID            := V_ID;
       Add_Rec_Open_Dog.NUM_DOG       := Number_Dog;
       Add_Rec_Open_Dog.SUMMA_DOG     := V_Sum_Dog;
       Add_Rec_Open_Dog.DATE_BEGIN    := Date_Open;
       Add_Rec_Open_Dog.DATE_END      := ADD_MONTHS (Date_Open, V_Month_Count);
       Add_Rec_Open_Dog.ID_CLIENT     := V_Id_Client;
       Add_Rec_Open_Dog.COLLECT_PLAN  := V_Collect_Plan;
       Add_Rec_Open_Dog.COLLECT_FACT  := V_Collect_Fact;
       
       --������� ������ ��� ������ �������� � ������� PR_CRED � ����� �� ����� ��������� �� ��������
       INSERT INTO PR_CRED VALUES Add_Rec_Open_Dog 
       RETURNING ID, NUM_DOG, SUMMA_DOG, DATE_BEGIN, DATE_END, ID_CLIENT, COLLECT_PLAN, COLLECT_FACT INTO Info_Opened_Dog;
       DBMS_OUTPUT.PUT_LINE (RPAD ('NEW_NUM_DOG', 20) || RPAD ('SUMMA_DOG', 20) || RPAD ('DATE_BEGIN', 20) || RPAD ('DATE_END', 20));
       DBMS_OUTPUT.PUT_LINE (RPAD (Info_Opened_Dog.NUM_DOG, 20) || RPAD (Info_Opened_Dog.SUMMA_DOG, 20) || 
                                  RPAD (Info_Opened_Dog.DATE_BEGIN, 20) || RPAD (Info_Opened_Dog.DATE_END, 20));
       DBMS_OUTPUT.PUT_LINE('--------------------------------------------------------------------');
       
       --���������� �������� '������ �������' �� ������ �������� � ������� ������� �������� �������� PLAN_OPER � ����� �� ����� ����������� ������
       Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
       Add_Rec_Plan_Oper.P_DATE         := Date_Open;
       Add_Rec_Plan_Oper.P_SUMMA        := V_Sum_Dog;
       Add_Rec_Plan_Oper.TYPE_OPER      := '������ �������';
       
       INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
       RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
       DBMS_OUTPUT.PUT_LINE (RPAD ('P_DATE', 20) || RPAD ('P_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
       DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
       DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------');
       
       --����������� ���������� ������ �� ���������������� ������� � ����������� �� ����� �������� � ����� �������
       CASE
       WHEN V_Sum_Dog < 300000 THEN
            IF  V_Month_Count <= 12 THEN 
                V_Percent_Rate := 9.9;
            ELSE 
                V_Percent_Rate := 12.9;
            END IF;    
       WHEN V_Sum_Dog >= 300000 AND V_Month_Count <= 12 THEN
            V_Percent_Rate := 8.9;     
       ELSE V_Percent_Rate := 11.9;     
       END CASE;     
       
       --���������� ���������� �������� ������� ������� �������� ������������ �������, ����� �� �������� ������� ����������� � �����
       V_Amount_Payment :=  Credit_Portfolio.Fun_Amount_Annuity_Payment (V_Sum_Dog, V_Percent_Rate, V_Month_Count);
       V_P_Date := Date_Open;
       
       FOR i IN 1..V_Month_Count
       LOOP
           --����������� �������� ������
           V_P_Date := ADD_MONTHS (V_P_Date, 1);
           
           --����������� ������� ��������� � ������� ������
           V_Percent_Credit := ROUND ((V_Sum_Dog - V_Body_Credit) * (V_Percent_Rate / 100 / 12), 2);
           
           --���������� �������� '��������� ���������' � ������� ������ �� ������ �������� � ������� �������� �������� PLAN_OPER � ����� �� ����� ����������� �����
           Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
           Add_Rec_Plan_Oper.P_DATE         := V_P_Date;
           Add_Rec_Plan_Oper.P_SUMMA        := V_Percent_Credit;
           Add_Rec_Plan_Oper.TYPE_OPER      := '��������� ���������';
           
           INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
           RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
           DBMS_OUTPUT.PUT_LINE (RPAD ('P_DATE', 20) || RPAD ('P_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
           DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
                                   
           --����������� ����� ��������� ���� ������� �� �������� ������������ �������
           V_Paid_Credit := V_Amount_Payment - V_Percent_Credit;
           
           --���������� �������� '��������� �������' � ������� ������ �� ������ �������� � ������� �������� �������� PLAN_OPER � ����� �� ����� ����������� �����
           Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
           Add_Rec_Plan_Oper.P_DATE         := V_P_Date;
           Add_Rec_Plan_Oper.P_SUMMA        := V_Paid_Credit;
           Add_Rec_Plan_Oper.TYPE_OPER      := '��������� �������';
           
           INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
           RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
           DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
           
           --����������� ������� ����������� ���� ������� � ����� �� �����
           V_Body_Credit := V_Body_Credit + V_Paid_Credit;
           DBMS_OUTPUT.PUT_LINE ('������ ����������� ���� ������� = '||ROUND (V_Body_Credit, 1));
           DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------');
       END LOOP;
       DBMS_OUTPUT.PUT_LINE ('������ � ������� ����� ��������� ������� � "' || Number_Dog || '" �� �����: ' || V_Sum_Dog ||
                             CHR (10)||'������ �� ' || V_Month_Count || ' ������� � ���������� ������� - '||V_Percent_Rate||'%'||
                             CHR (10)||'��� ������� - �����������, ������ ������������ ������� - '||V_Amount_Payment||' ���.');
  
  EXCEPTION
           WHEN VALUE_ERROR THEN
           DBMS_OUTPUT.PUT_LINE ('������ �������������� ����! ��� ������� � ���������� ���������� ��������� - NUMBER(10) � NUMBER(3)'||
                                  CHR (10)|| '��� �������� ��������� - DATE � �������: '||CHR (39)||'��-��-����'||CHR (39));
           --��������� ���������� ������������� ������� ������ ��������
           WHEN Num_Dog_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('������ ��������� ������ ��������! � "' || Number_Dog || '"- ������������ ������ ������'|| CHR (10)||
                               '������� ����� �������� � �������: '|| CHR (39)||'2021/n'||CHR (39)||' , ��� n - �����');
           GOTO end_of_proc; 
           
           --��������� ���������� ������������ ������ �������� � ����� �� ����� ������ ������������ � �������
           WHEN Unique_Constraint THEN
           DBMS_OUTPUT.PUT_LINE ('������! ����������� ������������! ������� ' ||Number_Dog|| ' ��� ����������'||
                                  CHR (10)|| '������������ � ������� ������ ���������:');
           FOR i IN  Nt_of_Exception.FIRST..Nt_of_Exception.LAST
             LOOP  
             DBMS_OUTPUT.PUT_LINE (Nt_of_Exception (i));
             END LOOP;
           GOTO end_of_proc;
           
           --��������� ���������� ������ �� ������� ���������� �������� ����� �������
           WHEN Range_Summa_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('������ ����� ��������! ���������� ������� ��������� ������� �� �����: '||V_Sum_Dog||
                                CHR (10)|| '�������� ����� �������: �� 30 000 �� 5 000 000');
           GOTO end_of_proc;
           
           --��������� ���������� ����������� ���� ������������ ��������
           WHEN Min_Date_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('������ ����! ���������� ������� ������� �����, �������������� ���� ���������� ��������'||
                                CHR (10)|| '���� ���������� ��������� ��������: '||Min_Date);
           GOTO end_of_proc;
           
           --��������� ���������� ������ �� ������� ���������� �������� ����� �������
           WHEN Range_Month_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('������ ������� ����� ���������������� �������! �������� ���������� ��������: �� 3 (3 ������) �� 60 (5 ���)');
           GOTO end_of_proc;
  RAISE; 
  --����� ���������� ��������� ��� ������ ����������������� ����������
  <<end_of_proc>>
  NULL;
 END;
       
 BEGIN
       OPEN_DOGOVOR ('2021/25', 300000, '25-02-2021', 6);
 END;
 
 SELECT * FROM PR_CRED;
 SELECT * FROM PLAN_OPER;
 
 --�������� ���� ��������� ��������� ��������  
 DELETE PLAN_OPER
 WHERE COLLECTION_ID = 6446261210042;
 /
 DELETE PLAN_OPER
 WHERE COLLECTION_ID = 6446261210153;
 /
 DELETE PR_CRED
 WHERE ID = 6446261211038;
 /
 DELETE PR_CRED
 WHERE ID =6446261212149;
 
 --�������� ��������� "���������� �������� '������ �������' " � ������� FACT_OPER
 CREATE OR REPLACE PROCEDURE Add_Issuance_Credit (Number_Dog IN PR_CRED.NUM_DOG%TYPE)
 IS
   CURSOR Cur_row_PR_CRED IS SELECT COLLECT_FACT, DATE_BEGIN, SUMMA_DOG FROM PR_CRED WHERE NUM_DOG = Number_Dog;
   Rec_Cur            Cur_row_PR_CRED%ROWTYPE;
   V_Exist_Col_Fact   FACT_OPER.COLLECTION_ID%TYPE := 0;
   Add_Rec_Fact_Oper  FACT_OPER%ROWTYPE;
   Info_Add_Rec_Fact  FACT_OPER%ROWTYPE;
   TYPE Nt_of_NUM_DOG IS TABLE OF PR_CRED.NUM_DOG%TYPE;
   Nt_Valid_NUM_DOG   Nt_of_NUM_DOG;
   Credit_Already_Issued  EXCEPTION;
   
 BEGIN
      --���������� ������ �������� ������ �������
      OPEN  Cur_row_PR_CRED; 
      FETCH Cur_row_PR_CRED INTO Rec_Cur;
      CLOSE Cur_row_PR_CRED;
      
      IF Rec_Cur.COLLECT_FACT IS NULL THEN
      RAISE NO_DATA_FOUND;               -- ����� ���������� ��� ���������� ��������� ��������
      END IF;
      
      --�������� ��������� ���������, �� ������� ��� �� ����������� �������� "������ �������"
      SELECT PC.NUM_DOG BULK COLLECT INTO Nt_Valid_NUM_DOG
      FROM PR_CRED PC
      LEFT JOIN FACT_OPER FO
      ON PC.COLLECT_FACT = FO.COLLECTION_ID
      WHERE FO.COLLECTION_ID IS NULL; 
      
      --�������� �� ������� ��� ��������� ������� ��� ��������� ��������
      IF Number_Dog NOT MEMBER OF Nt_Valid_NUM_DOG THEN
      RAISE Credit_Already_Issued;               -- ����� ���������� ��� ������� � ������� ��������� ������� ��� ��������� ��������
      END IF;

      --���������� ������ ����������� �������� � ������� � ������� ����������� �������� FACT_OPER � ������� �� ����� ����������� ������
      Add_Rec_Fact_Oper.COLLECTION_ID := Rec_Cur.COLLECT_FACT;
      Add_Rec_Fact_Oper.F_DATE := Rec_Cur.DATE_BEGIN;
      Add_Rec_Fact_Oper.F_SUMMA := Rec_Cur.SUMMA_DOG;
      Add_Rec_Fact_Oper.TYPE_OPER := '������ �������';
      
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD ('COLLECTION_ID', 20) || RPAD ('F_DATE', 20) || RPAD ('F_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20));
 EXCEPTION
    -- ��������� ����������, ���� ���������� ������ �������� �� ���������� ��� ������ ������������ ������ ������
    WHEN NO_DATA_FOUND THEN
         DBMS_OUTPUT.PUT_LINE ('������! ���������� �������� � "' || Number_Dog || '" �� ���������� � �������'|| CHR (10)||
                                   '������� ����� �������� � �������: '|| CHR (39)||'2021/n'||CHR (39)||' , ��� n - �����');
                   
    --��������� ����������, ���� �� ������� �������� ��� ����� ������
    WHEN Credit_Already_Issued THEN
        --����� �� ����� ������������ ��������� � ��������� ��������� ��� ���������� �������� "������ �������"
        DBMS_OUTPUT.PUT_LINE ('������! �� ���������� �������� � "' || Number_Dog || '" ��� ����������� �������� "������ �������"');
        IF Nt_Valid_NUM_DOG IS EMPTY THEN
           DBMS_OUTPUT.PUT_LINE ('� ������� ��� ��������� ��������� ��� ���������� �������� "������ �������"'||
                                  CHR (10)|| '�������� � ������� ����� ������� ���������� OPEN_DOGOVOR');
        ELSE 
             DBMS_OUTPUT.PUT_LINE ('��������� �������� ��� ���������� �������� "������ �������":');
             FOR i IN  Nt_Valid_NUM_DOG.FIRST..Nt_Valid_NUM_DOG.LAST
             LOOP  
             DBMS_OUTPUT.PUT_LINE (Nt_Valid_NUM_DOG (i));
             END LOOP; 
         END IF;
         GOTO end_of_proc;
    RAISE;
    --����� ���������� ��������� ��� ������ ����������������� ����������
    <<end_of_proc>>
    NULL;
 END;
 
BEGIN
      Add_Issuance_Credit ('2021/10');
END;

SELECT * FROM FACT_OPER;

--�������� ��������� ���� ��������
DELETE FACT_OPER
WHERE COLLECTION_ID = 6446261210039;

--�������� ��������� "���������� �������� '��������� �������' " � ������� FACT_OPER
CREATE OR REPLACE PROCEDURE Add_Paid_Credit (Number_Dog IN PR_CRED.NUM_DOG%TYPE,
                                             Summa IN VARCHAR2)
 IS
   V_Summa            NUMBER (12,2);
   V_Collect_Plan     PR_CRED.COLLECT_PLAN%TYPE;
   V_Collect_Fact     PR_CRED.COLLECT_FACT%TYPE;
   V_F_Date           FACT_OPER.F_DATE%TYPE;
   TYPE Nt_of_Col_Id  IS TABLE OF FACT_OPER.COLLECTION_ID%TYPE;
   Nt_Close_Col_Id    Nt_of_Col_Id; -- ��������� �������� (����������) ��������
   V_Paid_Credit      NUMBER (12,2);
   V_Percent_Credit   NUMBER (12,2);
   V_Ostatok_Credit   NUMBER (12,2);
   V_Path_Credit      NUMBER (12,2);
   V_Ostatok          NUMBER (12,2);
   Add_Rec_Fact_Oper  FACT_OPER%ROWTYPE;
   Info_Add_Rec_Fact  FACT_OPER%ROWTYPE;
   Min_Summa_Exc      EXCEPTION;
   Credit_Close_Exc   EXCEPTION;
   
 BEGIN
      --�������� ���������� ��������� ��������� Summa �� �������� ��� ������ � ����� ����������, ���� Summa �������� �������
      V_Summa := TO_NUMBER (Summa);
      
      --����������� COLLECTION_ID ��� ��������� �������� � ����� ����������, ���� ������� �� ������
      SELECT COLLECT_PLAN, COLLECT_FACT  INTO V_Collect_Plan, V_Collect_Fact FROM PR_CRED
      WHERE NUM_DOG = Number_Dog;
      
      --����������� ������� ��������� ������� �� ��������� �������� � ����� ����������, ���� �� ����� �������� ��� �� ����� ������
      SELECT F_SUMMA INTO V_Ostatok_Credit FROM FACT_OPER
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = '������ �������';
            
      --�������� �� ������� ��� ����������� ������� �� ���������� ��������
      SELECT COLLECTION_ID BULK COLLECT INTO Nt_Close_Col_Id FROM FACT_OPER
      WHERE TYPE_OPER = '������ �������'; 
      
      IF V_Collect_Fact MEMBER OF Nt_Close_Col_Id THEN
      RAISE Credit_Close_Exc;               -- ����� ���������� ��� ������� � ������� ��� ����������� (���������) ������� ��� ��������� ��������
      END IF;
      
      --����������� F_Date ��� ��������� ��������
      SELECT  MIN (PO.P_DATE) INTO V_F_Date
      FROM      PLAN_OPER PO
      LEFT JOIN FACT_OPER FO
      ON PO.P_DATE = FO.F_DATE
      WHERE PO.COLLECTION_ID = V_Collect_Plan AND
            PO.TYPE_OPER = '��������� �������' AND
            (PO.P_DATE IS NULL OR FO.F_DATE IS NULL);
      
      --����������� P_SUMMA ��� ��������� ��������� ���� �������
      SELECT P_SUMMA INTO V_Paid_Credit FROM PLAN_OPER
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = '��������� �������' AND
            P_DATE = V_F_Date;     
      
      --����������� F_SUMMA ��� ��������� ���������      
      SELECT P_SUMMA INTO V_Percent_Credit FROM PLAN_OPER
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = '��������� ���������' AND
            P_DATE = V_F_Date;     
      
      -- �������� ��������� Summa �� ����������� �������� ������� � ������� ������ ��� ��������� ��������
      IF    V_Summa != 1 AND V_Summa != 2 AND  V_Summa  <  (V_Paid_Credit + V_Percent_Credit) THEN
      RAISE Min_Summa_Exc;   -- ����� ����������, ���� ��������� ����� ������ ��������� ������� � ������� ������           
      END IF;
      
      --���������� ������ '��������� ���������' � ������� FACT_OPER � ����� �� ����� ����������� ����� ��� ��������� ��������
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Percent_Credit;
      Add_Rec_Fact_Oper.TYPE_OPER      := '��������� ���������';
      
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD ('COLLECTION_ID', 20) || RPAD ('F_DATE', 20) || RPAD ('F_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20)); 
            
      --����������� ������� ���������� ����� ���� �������
      SELECT SUM (F_SUMMA) INTO V_Path_Credit FROM FACT_OPER
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = '��������� �������';      
      
      --����������� F_SUMMA � ������ ���������� � ����������� �� ��������� Summa       
      CASE
      --���� ������� "1", �� ����� �������� ���� ������� � ������� ���������� ��������� ������� V_Paid_Credit 
      WHEN V_Summa = 1 THEN
      --����������� ������� ������� �������������
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - V_Paid_Credit;
      --���� ������� "2", �� ����� ��������� �������� ���� ������� 
      WHEN V_Summa = 2 THEN
           V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := 0;
      --����������� ������� ��������� ���� ������� V_Paid_Credit � ������� ������� �������������, ���� ������� ������������ Summa, �� ����������� ������� ������� �������������
      WHEN (V_Ostatok_Credit - V_Path_Credit) > (V_Summa - V_Percent_Credit) THEN
           V_Paid_Credit := V_Summa - V_Percent_Credit;
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - V_Paid_Credit;
           
      --������ ��������� ���� ������� � ����������� ������� ��������� ������� �������� ��� ������������� �������� ��� ��������� Summa > ������� ������� �������������
      ELSE V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := V_Summa - V_Percent_Credit - V_Paid_Credit;
      END CASE;     
      
      --���������� ������ '��������� �������' � ������� FACT_OPER � ����� �� ����� ����������� ����� ��� ��������� ��������
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Paid_Credit;
      IF   V_Ostatok IS NULL THEN
           Add_Rec_Fact_Oper.TYPE_OPER      := '��������� �������';
      ELSE
           Add_Rec_Fact_Oper.TYPE_OPER      := '������ �������';
      END IF;     
           
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20));
      DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------------------');
      --����� �� ����� ���������� � ��������� ���������� �������� ����� �������� "��������� �������"
      IF   V_Ostatok IS NULL THEN
           DBMS_OUTPUT.PUT_LINE ('������� �� ���� ������������� ������� �� �������� '|| Number_Dog || ' ����������: '|| V_Ostatok_Credit);
      ELSE 
           DBMS_OUTPUT.PUT_LINE ('������ �� �������� '|| Number_Dog || ' ��������� �������');
           IF V_Ostatok != 0 THEN
           DBMS_OUTPUT.PUT_LINE ('������� ��������� �������� ������ � ������� '||V_Ostatok||' ���������� �� ��� ��������� ����');
           END IF;                 
      END IF;
 EXCEPTION
           -- ��������� ����������, ���� ���������� ������ �������� �� ���������� ��� ������ ������������ ������ ������
           WHEN VALUE_ERROR THEN
                DBMS_OUTPUT.PUT_LINE ('������ ORA-06502! VALUE_ERROR!' || CHR (10)||
                                      '������ �������� ��������� �������� ������, ������� ����� � ��������� �� ����� 2-� ������ ����� �������');
           WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE ('������! ���������� �������� � "' || Number_Dog || '" �� ���������� � ������� ��� �� �������� ��� �� ����� ������'|| CHR (10)||
                               '������� ����� �������� � �������: '|| CHR (39)||'2021/n'||CHR (39)||' , ��� n - �����');
           
           WHEN Credit_Close_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('������! �� ���������� �������� � "' || Number_Dog || '" ������ ��� ��������� ������� ');
           GOTO end_of_proc; 
           
           WHEN Min_Summa_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('������ ��������� ����� ���������!' || CHR (10)|| 
                                 '��������� ����� ' || Summa || ' �� ������ ���� ������ �������� ������������� ������� � ������� ������'|| CHR (10)||
                                 '������������ ������ � ������� ������ ��� �������� '|| Number_Dog ||' ����������: '|| (V_Paid_Credit + V_Percent_Credit));
           GOTO end_of_proc;                     
  RAISE; 
  --����� ���������� ��������� ��� ������ ����������������� ����������
  <<end_of_proc>>
  NULL;
 END;

BEGIN
      Add_Paid_Credit ('2021/1', 30000);
END;

--�������� ��������� ���� ��������
DELETE FACT_OPER
WHERE COLLECTION_ID = 6120475936893 AND
      F_DATE = '03-03-2021';
      
           

DROP TABLE PLAN_OPER_1;
/
DROP TABLE FACT_OPER_1;
/
CREATE TABLE PLAN_OPER_1 AS SELECT * FROM PLAN_OPER;
/
CREATE TABLE FACT_OPER_1 AS SELECT * FROM FACT_OPER;


DROP TABLE PLAN_OPER_AUDIT_1;

CREATE TABLE PLAN_OPER_AUDIT_1 (
                               COLLECTION_ID  NUMBER (15),
                               P_DATE         DATE,
                               P_SUMMA        NUMBER (12,2),
                               TYPE_OPER      VARCHAR2 (200),
                               USER_NAME      VARCHAR2 (50),
                               DATE_OPEN      TIMESTAMP,
                               TYPE_OPERATION VARCHAR2 (20)
                              );
 
 --DROP TRIGGER Tr_Audit_PLAN_OPER_1;
 
 SELECT * FROM PLAN_OPER_AUDIT_1;
 
 TRUNCATE TABLE PLAN_OPER_AUDIT_1;
 
 --�������� �������� �� �������/�������� ������ � ������� PR_CRED � ����������� ������� � ������� PR_CRED_AUDIT
 CREATE OR REPLACE TRIGGER Tr_Audit_PLAN_OPER_1
 AFTER INSERT OR DELETE OR UPDATE
 ON PLAN_OPER_1 
 FOR EACH ROW
 DECLARE
   V_User_Name      VARCHAR2(50);
   V_Type_Operation VARCHAR2 (20);
 BEGIN   
   -- ����� ������� username, ��������������� INSERT � �������
   SELECT USER INTO V_User_Name
   FROM DUAL;   
   -- �������� ������ � ������� PR_CRED_AUDIT
   CASE 
   WHEN INSERTING THEN
        V_Type_Operation := 'INSERT';
        INSERT INTO PLAN_OPER_AUDIT_1 (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:new.COLLECTION_ID, :new.P_DATE, :new.P_SUMMA, :new.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   WHEN DELETING THEN
        V_Type_Operation := 'DELETE';
        INSERT INTO PLAN_OPER_AUDIT_1 (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:old.COLLECTION_ID, :old.P_DATE, :old.P_SUMMA, :old.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
   WHEN UPDATING ('P_SUMMA') THEN
        V_Type_Operation := 'BEFORE UPDATE';
        INSERT INTO PLAN_OPER_AUDIT_1 (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:old.COLLECTION_ID, :old.P_DATE, :old.P_SUMMA, :old.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);
        V_Type_Operation := 'AFTER UPDATE';
        INSERT INTO PLAN_OPER_AUDIT_1 (COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER, USER_NAME, DATE_OPEN, TYPE_OPERATION)
        VALUES (:new.COLLECTION_ID, :new.P_DATE, :new.P_SUMMA, :new.TYPE_OPER, V_User_Name, systimestamp, V_Type_Operation);  
   END CASE;
 END;



CREATE OR REPLACE PROCEDURE Add_Paid_Credit_1 (Number_Dog IN PR_CRED.NUM_DOG%TYPE,
                                               Summa IN VARCHAR2)
 IS
   V_Summa              NUMBER (12,2);
   V_Collect_Plan       PR_CRED.COLLECT_PLAN%TYPE;
   V_Collect_Fact       PR_CRED.COLLECT_FACT%TYPE;
   V_F_Date             FACT_OPER.F_DATE%TYPE;
   TYPE Nt_of_Col_Id    IS TABLE OF FACT_OPER.COLLECTION_ID%TYPE;
   Nt_Close_Col_Id      Nt_of_Col_Id; -- ��������� �������� (����������) ��������
   V_Plan_Paid_Credit   NUMBER (12,2);
   V_Paid_Credit        NUMBER (12,2);
   V_Percent_Credit     NUMBER (12,2);
   V_Ostatok_Credit     NUMBER (12,2);
   V_Path_Credit        NUMBER (12,2);
   V_Ostatok            NUMBER (12,2);
   V_Percent            NUMBER (5,2);
   TYPE Aa_P_SUMMA IS TABLE OF PLAN_OPER_1.P_SUMMA%TYPE INDEX BY PLS_INTEGER;
   Aa_Change_Sum_Cred   Aa_P_SUMMA;
   Aa_Change_Sum_Perc   Aa_P_SUMMA;
   TYPE Nt_P_DATE IS TABLE OF PLAN_OPER_1.P_DATE%TYPE;
   Nt_Change_P_DATE     Nt_P_DATE;
   Add_Rec_Fact_Oper    FACT_OPER%ROWTYPE;
   Info_Add_Rec_Fact    FACT_OPER%ROWTYPE;
   Min_Summa_Exc        EXCEPTION;
   Credit_Close_Exc     EXCEPTION;
   
 BEGIN
      --�������� ���������� ��������� ��������� Summa �� �������� ��� ������ � ����� ����������, ���� Summa �������� �������
      V_Summa := TO_NUMBER (Summa);
      
      --����������� COLLECTION_ID ��� ��������� �������� � ����� ����������, ���� ������� �� ������
      SELECT COLLECT_PLAN, COLLECT_FACT  INTO V_Collect_Plan, V_Collect_Fact FROM PR_CRED_1
      WHERE NUM_DOG = Number_Dog;
      
      --����������� ������� ��������� ������� �� ��������� �������� � ����� ����������, ���� �� ����� �������� ��� �� ����� ������
      SELECT F_SUMMA INTO V_Ostatok_Credit FROM FACT_OPER_1
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = '������ �������';
            
      --�������� �� ������� ��� ����������� ������� �� ���������� ��������
      SELECT COLLECTION_ID BULK COLLECT INTO Nt_Close_Col_Id FROM FACT_OPER_1
      WHERE TYPE_OPER = '������ �������'; 
      
      IF V_Collect_Fact MEMBER OF Nt_Close_Col_Id THEN
      RAISE Credit_Close_Exc;               -- ����� ���������� ��� ������� � ������� ��� ����������� (���������) ������� ��� ��������� ��������
      END IF;
      
      --����������� F_Date ��� ��������� ��������
      SELECT  MIN (PO.P_DATE) INTO V_F_Date
      FROM      PLAN_OPER_1 PO
      LEFT JOIN FACT_OPER_1 FO
      ON PO.P_DATE = FO.F_DATE
      WHERE PO.COLLECTION_ID = V_Collect_Plan AND
            PO.TYPE_OPER = '��������� �������' AND
            (PO.P_DATE IS NULL OR FO.F_DATE IS NULL);
      
      --����������� P_SUMMA ��� ��������� ��������� ���� �������
      SELECT P_SUMMA INTO V_Plan_Paid_Credit FROM PLAN_OPER_1
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = '��������� �������' AND
            P_DATE = V_F_Date;     
      
      --����������� F_SUMMA ��� ��������� ���������      
      SELECT P_SUMMA INTO V_Percent_Credit FROM PLAN_OPER_1
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = '��������� ���������' AND
            P_DATE = V_F_Date;     
      
      -- �������� ��������� Summa �� ����������� �������� ������� � ������� ������ ��� ��������� ��������
      IF    V_Summa != 1 AND V_Summa != 2 AND  V_Summa  <  (V_Plan_Paid_Credit + V_Percent_Credit) THEN
      RAISE Min_Summa_Exc;   -- ����� ����������, ���� ��������� ����� ������ ��������� ������� � ������� ������           
      END IF;
      
      --���������� ������ '��������� ���������' � ������� FACT_OPER_1 � ����� �� ����� ����������� ����� ��� ��������� ��������
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Percent_Credit;
      Add_Rec_Fact_Oper.TYPE_OPER      := '��������� ���������';
      
      INSERT INTO FACT_OPER_1 VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD ('COLLECTION_ID', 20) || RPAD ('F_DATE', 20) || RPAD ('F_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20)); 
            
      --����������� ������� ���������� ����� ���� �������
      SELECT SUM (F_SUMMA) INTO V_Path_Credit FROM FACT_OPER_1
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = '��������� �������';      
      
      --����������� F_SUMMA � ������ ���������� � ����������� �� ��������� Summa       
      CASE
      --���� ������� "1", �� ����� �������� ���� ������� � ������� ���������� ��������� ������� V_Paid_Credit 
      WHEN V_Summa = 1 THEN
      --����������� ������� ������� �������������
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - V_Plan_Paid_Credit;
      --���� ������� "2", �� ����� ��������� �������� ���� ������� 
      WHEN V_Summa = 2 THEN
           V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := 0;
      --����������� ������� ��������� ���� ������� V_Paid_Credit � ������� ������� �������������, ���� ������� ������������ Summa, �� ����������� ������� ������� �������������
      WHEN (V_Ostatok_Credit - V_Path_Credit) > (V_Summa - V_Percent_Credit) THEN
           V_Percent := ROUND (V_Percent_Credit / (V_Ostatok_Credit - V_Path_Credit) * 12 * 100, 2);
           V_Paid_Credit := V_Summa - V_Percent_Credit;
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - V_Paid_Credit;
           
      --������ ��������� ���� ������� � ����������� ������� ��������� ������� �������� ��� ������������� �������� ��� ��������� Summa > ������� ������� �������������
      ELSE V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := V_Summa - V_Percent_Credit - V_Paid_Credit;
      END CASE;     
      
      --���������� ������ '��������� �������' � ������� FACT_OPER_1 � ����� �� ����� ����������� ����� ��� ��������� ��������
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Paid_Credit;
      IF   V_Ostatok IS NULL THEN
           Add_Rec_Fact_Oper.TYPE_OPER      := '��������� �������';
      ELSE
           Add_Rec_Fact_Oper.TYPE_OPER      := '������ �������';
      END IF;     
           
      INSERT INTO FACT_OPER_1 VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20));
      DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------------------');
      --����� �� ����� ���������� � ��������� ���������� �������� ����� �������� "��������� �������"
      IF   V_Ostatok IS NULL THEN
           DBMS_OUTPUT.PUT_LINE ('������� �� ���� ������������� ������� �� �������� '|| Number_Dog || ' ����������: '|| V_Ostatok_Credit);
      ELSE 
           DBMS_OUTPUT.PUT_LINE ('������ �� �������� '|| Number_Dog || ' ��������� �������');
           IF V_Ostatok != 0 THEN
           DBMS_OUTPUT.PUT_LINE ('������� ��������� �������� ������ � ������� '||V_Ostatok||' ���������� �� ��� ��������� ����');
           END IF;                 
      END IF;
      
      --���������� ��� ��������� ������� ����� �������� �������� (��������� ���� ������� � ��������) ��� �������� ������� � ��������� ����������
      IF V_Summa > (V_Plan_Paid_Credit + V_Percent_Credit) AND V_Summa < (V_Ostatok_Credit + V_Paid_Credit) THEN
      
        --������������ ��������� ����������� ����������� �������� �� ��������� ���� �������
        SELECT P_SUMMA BULK COLLECT INTO Aa_Change_Sum_Cred FROM PLAN_OPER_1
        WHERE COLLECTION_ID = V_Collect_Plan  AND
              P_DATE > V_F_Date AND
              TYPE_OPER = '��������� �������';    
              
        --��������� ����� ��������� ���� ������� � ������� �������� �������� � ���� ������������ ���������� �������
        UPDATE PLAN_OPER_1
        SET P_SUMMA = (V_Summa - V_Percent_Credit)
        WHERE COLLECTION_ID = V_Collect_Plan AND
              TYPE_OPER = '��������� �������' AND
              P_DATE = V_F_Date;

        --������������ ��������� ��� ������� �������� �� ������ ������� �������� ��������
        SELECT P_DATE BULK COLLECT INTO Nt_Change_P_DATE FROM PLAN_OPER_1
        WHERE COLLECTION_ID = V_Collect_Plan AND
              TYPE_OPER = '��������� �������' AND
              P_DATE > V_F_Date;

        --��������, ���� ������� ����� ���� ������� ������
        IF Nt_Change_P_DATE.COUNT = 1 THEN
        
            V_Paid_Credit := V_Ostatok_Credit;
            V_Percent_Credit := V_Ostatok_Credit * V_Percent / 100 / 12;
             
            UPDATE PLAN_OPER_1
            SET P_SUMMA = V_Paid_Credit
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� �������' AND
                  P_DATE = Nt_Change_P_DATE (1);
                   
            UPDATE PLAN_OPER_1
            SET P_SUMMA = V_Percent_Credit
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� ���������' AND
                  P_DATE = Nt_Change_P_DATE (1);      
        
         --��������� ������� ����� �������� ��������, ���� �� ���������� �������� ������� ������������������
         ELSIF Aa_Change_Sum_Cred (1) = Aa_Change_Sum_Cred (2) THEN

            V_Paid_Credit := ROUND (V_Ostatok_Credit / Nt_Change_P_DATE.COUNT, 2);

            UPDATE PLAN_OPER_1
            SET P_SUMMA = V_Paid_Credit
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� �������' AND
                  P_DATE > V_F_Date;

            --������������ ��������� ����� �������� ��������� ��������� ����� ���������� �������
            FOR i IN Nt_Change_P_DATE.FIRST..Nt_Change_P_DATE.LAST
            LOOP
                V_Percent_Credit := V_Ostatok_Credit * V_Percent / 100 / 12;
                Aa_Change_Sum_Perc(i) := V_Percent_Credit;
                V_Ostatok_Credit := V_Ostatok_Credit - V_Paid_Credit;
            END LOOP;
        
            FORALL j IN Aa_Change_Sum_Perc.FIRST..Aa_Change_Sum_Perc.LAST
            UPDATE PLAN_OPER_1
            SET P_SUMMA = Aa_Change_Sum_Perc (j)
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� ���������' AND
                  P_DATE = Nt_Change_P_DATE (j);
        
        --��������� ������� ����� �������� ��������, ���� �� ���������� �������� ������� �����������
       ELSE
            V_Plan_Paid_Credit := Credit_Portfolio.Fun_Amount_Annuity_Payment (V_Ostatok_Credit, V_Percent, Nt_Change_P_DATE.COUNT);
            
            FOR i IN Nt_Change_P_DATE.FIRST..Nt_Change_P_DATE.LAST
            LOOP
                V_Percent_Credit := V_Ostatok_Credit * V_Percent / 100 / 12;
                Aa_Change_Sum_Perc(i) := V_Percent_Credit;
                V_Paid_Credit := V_Plan_Paid_Credit - V_Percent_Credit;
                Aa_Change_Sum_Cred(i) := V_Paid_Credit;
                V_Ostatok_Credit := V_Ostatok_Credit - V_Paid_Credit;
            END LOOP;
            
            FORALL j IN Aa_Change_Sum_Cred.FIRST..Aa_Change_Sum_Cred.LAST
            UPDATE PLAN_OPER_1
            SET P_SUMMA = Aa_Change_Sum_Cred (j)
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� �������' AND
                  P_DATE = Nt_Change_P_DATE (j);
            
            FORALL j IN Aa_Change_Sum_Perc.FIRST..Aa_Change_Sum_Perc.LAST
            UPDATE PLAN_OPER_1
            SET P_SUMMA = Aa_Change_Sum_Perc (j)
            WHERE COLLECTION_ID = V_Collect_Plan AND
                  TYPE_OPER = '��������� ���������' AND
                  P_DATE = Nt_Change_P_DATE (j);     
        END IF;
      END IF;

 EXCEPTION
           -- ��������� ����������, ���� ���������� ������ �������� �� ���������� ��� ������ ������������ ������ ������
           WHEN VALUE_ERROR THEN
                DBMS_OUTPUT.PUT_LINE ('������ ORA-06502! VALUE_ERROR!' || CHR (10)||
                                      '������ �������� ��������� �������� ������, ������� ����� � ��������� �� ����� 2-� ������ ����� �������');
           WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE ('������! ���������� �������� � "' || Number_Dog || '" �� ���������� � ������� ��� �� �������� ��� �� ����� ������'|| CHR (10)||
                               '������� ����� �������� � �������: '|| CHR (39)||'2021/n'||CHR (39)||' , ��� n - �����');
           
           WHEN Credit_Close_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('������! �� ���������� �������� � "' || Number_Dog || '" ������ ��� ��������� ������� ');
           GOTO end_of_proc; 
           
           WHEN Min_Summa_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('������ ��������� ����� ���������!' || CHR (10)|| 
                                 '��������� ����� ' || Summa || ' �� ������ ���� ������ �������� ������������� ������� � ������� ������'|| CHR (10)||
                                 '������������ ������ � ������� ������ ��� �������� '|| Number_Dog ||' ����������: '|| (V_Plan_Paid_Credit + V_Percent_Credit));
           GOTO end_of_proc;                     
  RAISE; 
  --����� ���������� ��������� ��� ������ ����������������� ����������
  <<end_of_proc>>
  NULL;
 END;

BEGIN
      Add_Paid_Credit_1 ('2021/21', 40000);
END;

 SELECT * FROM PLAN_OPER_AUDIT_1;
 
 select * from plan_oper
 where collection_id = 6432340347875;
 
 select * from plan_oper_1
 where collection_id = 6432340347875;
 
 --��������� �������� �� ������� FACT_OPER ���������� ������� � ������� ���������� �������� P_SUMMA � ������� �������� ��������
 CREATE OR REPLACE PROCEDURE Proc_Delete_Rec_Fact_Oper (Number_Dog IN PR_CRED.NUM_DOG%TYPE, Date_Oper IN DATE) IS
 V_Collect_Plan          PLAN_OPER.COLLECTION_ID%TYPE;
 V_Collect_Fact          FACT_OPER.COLLECTION_ID%TYPE;
 TYPE Nt_P_SUMMA IS TABLE OF PLAN_OPER_AUDIT_1.P_SUMMA%TYPE;
 Nt_Change_Sum_Cred      Nt_P_SUMMA;
 Nt_Change_Sum_Perc      Nt_P_SUMMA;
 TYPE Nt_P_DATE IS TABLE OF PLAN_OPER_AUDIT_1.P_DATE%TYPE;
 Nt_Change_P_DATE_Cred   Nt_P_DATE;
 Nt_Change_P_DATE_Perc   Nt_P_DATE;
 V_P_SUMMA               FACT_OPER.F_SUMMA%TYPE ;
 V_Count_Date            NUMBER (3);
 V_Date                  TIMESTAMP;
 
 BEGIN

    SELECT COLLECT_PLAN, COLLECT_FACT INTO V_Collect_Plan, V_Collect_Fact  FROM PR_CRED_1
    WHERE NUM_DOG = Number_Dog;

    DELETE FACT_OPER_1
    WHERE COLLECTION_ID = V_Collect_Fact AND
          F_DATE = Date_Oper;
    
    --������������ ��������� ��� ��������� ���� �������, ���������� ��������� ����� �������� ������ ���������� ���������
    SELECT P_DATE BULK COLLECT INTO Nt_Change_P_DATE_Cred FROM PLAN_OPER_1
    WHERE COLLECTION_ID = V_Collect_Plan AND
          TYPE_OPER = '��������� �������' AND
          P_DATE >= Date_Oper;
      
    --������������ ��������� ��� ��������� ���������, ���������� ��������� ����� �������� ������ ���������� ���������
    SELECT P_DATE BULK COLLECT INTO Nt_Change_P_DATE_Perc FROM PLAN_OPER_1
    WHERE COLLECTION_ID = V_Collect_Plan AND
          TYPE_OPER = '��������� ���������' AND
          P_DATE > Date_Oper;      

    --����������� ����/������� ����������� ��������� (������ UPDATE) ������� �������� �������� �� ��������� �������� ��� ������ ������ �� �������
    SELECT a.* INTO V_Date FROM (SELECT DATE_OPEN  FROM PLAN_OPER_AUDIT_1
                                 WHERE P_DATE = Date_Oper AND
                                       TYPE_OPER = '��������� �������' AND
                                       TYPE_OPERATION = 'BEFORE UPDATE'
                                 ORDER BY DATE_OPEN desc) a
    WHERE rownum = 1;

    --����������� ���������� ��������� �������� ���� ������� ��� ��������� ����������
    V_Count_Date := Nt_Change_P_DATE_Cred.COUNT;

    --������������ ��������� ���������� (�� ���������� ���������� ���������) �������� P_SUMMA ��� ���� ������� �� ������� ������ PLAN_OPER_AUDIT_1
    SELECT a.* BULK COLLECT INTO Nt_Change_Sum_Cred FROM (SELECT P_SUMMA FROM PLAN_OPER_AUDIT_1
                                                          WHERE COLLECTION_ID = V_Collect_Plan  AND
                                                                P_DATE >= Date_Oper AND
                                                                TYPE_OPER = '��������� �������' AND
                                                                TYPE_OPERATION = 'BEFORE UPDATE' AND
                                                                DATE_OPEN >= V_Date
                                                                ORDER BY DATE_OPEN) a 
    WHERE rownum <= V_Count_Date;

    --����������� ���������� ��������� �������� ��������� ��� ��������� ����������
    V_Count_Date := Nt_Change_P_DATE_Perc.COUNT;

    --������������ ��������� ���������� (�� ���������� ���������� ���������) �������� P_SUMMA ��� ��������� �� ������� ������ PLAN_OPER_AUDIT_1
    SELECT a.* BULK COLLECT INTO Nt_Change_Sum_Perc FROM (SELECT P_SUMMA   FROM PLAN_OPER_AUDIT_1
                                                          WHERE COLLECTION_ID = V_Collect_Plan  AND
                                                                P_DATE > Date_Oper AND
                                                                TYPE_OPER = '��������� ���������' AND
                                                                TYPE_OPERATION = 'BEFORE UPDATE' AND
                                                                DATE_OPEN >= V_Date
                                                                ORDER BY DATE_OPEN) a 
     WHERE rownum <= V_Count_Date;

    --����������� ���������� (�� ���������� ���������� ���������) �������� P_SUMMA ��� ���� ������� � ��������� � ������� �������� �������� 
    FORALL j IN Nt_Change_Sum_Cred.FIRST..Nt_Change_Sum_Cred.LAST
    UPDATE PLAN_OPER_1
    SET P_SUMMA = Nt_Change_Sum_Cred (j)
    WHERE COLLECTION_ID = V_Collect_Plan  AND
          P_DATE = Nt_Change_P_DATE_Cred (j) AND
          TYPE_OPER = '��������� �������';

    FORALL j IN Nt_Change_Sum_Perc.FIRST..Nt_Change_Sum_Perc.LAST
    UPDATE PLAN_OPER_1
    SET P_SUMMA = Nt_Change_Sum_Perc (j)
    WHERE COLLECTION_ID = V_Collect_Plan  AND
          P_DATE = Nt_Change_P_DATE_Perc (j) AND
          TYPE_OPER = '��������� ���������';
 END;

 BEGIN
      Proc_Delete_Rec_Fact_Oper ('2021/21', '20-02-2021');
 END;


select * from plan_oper
 where collection_id = 6432340347875;
 
 select * from plan_oper_1
 where collection_id = 6432340347875;









