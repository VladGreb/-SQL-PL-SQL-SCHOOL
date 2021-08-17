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
    --Функция подсчета суммы предстоящих процентов к погашению  для заданного договора на заданную дату
    FUNCTION REST_OF_PERCENT (DATE_STATUS IN DATE, Collect_P_id IN FACT_OPER.F_SUMMA%TYPE, Collect_F_id IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE;
    
     --Функция подсчета остатка ссудной задолженности для заданного договора на заданную дату
    FUNCTION REST_OF_CREDIT (DATE_STATUS IN DATE, Collect_id_F IN FACT_OPER.F_SUMMA%TYPE) 
    RETURN FACT_OPER.F_SUMMA%TYPE;
    
    --Функция расчета размера аннуитетного платежа
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
               TYPE_OPER = 'Погашение процентов';
          
         SELECT SUM (F_SUMMA) INTO V_Paid_Percent
         FROM FACT_OPER
         WHERE F_DATE <= DATE_STATUS AND
               COLLECTION_ID = Collect_F_id AND
               TYPE_OPER = 'Погашение процентов'; 
               
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
               TYPE_OPER = 'Выдача кредита';
          
         SELECT SUM (F_SUMMA) INTO V_Paid_Credit
         FROM FACT_OPER
         WHERE F_DATE <= DATE_STATUS AND
               COLLECTION_ID = Collect_id_F AND
               TYPE_OPER = 'Погашение кредита'; 
          
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

--Вывод требуемого отчета на экран SQL через конвеерную функцию

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
--Создаем коллекцию типа nested table 
CREATE TYPE NT_Credit AS TABLE OF Type_Credit_Object;
/
CREATE OR REPLACE FUNCTION Credit_Function (Date_Otchot IN DATE)
          RETURN NT_Credit PIPELINED AS
BEGIN
    FOR i IN ( SELECT NUM_DOG, CL_NAME, SUMMA_DOG, DATE_BEGIN, DATE_END, 
                      Credit_Portfolio.REST_OF_CREDIT (Date_Otchot, COLLECT_FACT) REST_CREDIT, 
                      Credit_Portfolio.REST_OF_PERCENT (Date_Otchot, COLLECT_PLAN, COLLECT_FACT) REST_PERCENT, Date_Otchot Дата_отчета  
             FROM PR_CRED PR
             JOIN CLIENT CL
             ON PR.ID_CLIENT = CL.ID
             WHERE DATE_BEGIN < Date_Otchot
             ORDER BY DATE_BEGIN, NUM_DOG)
    LOOP
    PIPE ROW (Type_Credit_Object (i.NUM_DOG, i.CL_NAME, i.SUMMA_DOG, i.DATE_BEGIN, i.DATE_END, i.REST_CREDIT, i.REST_PERCENT, i.Дата_отчета));    
    END LOOP;
  RETURN;
END;

--Вызов функции от заданной пользователем даты для просмотра отчета
SELECT * FROM TABLE (Credit_Function('30-10-2020'));
 
--Формирование отчета методом коллекций и курсора
--Создание временной таблицы для сохранения результата отчёта
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
 --Определение нужных для отчёта столбцов из таблиц с помощью курсора
 CURSOR Cur_Credit_Client_Info IS SELECT PC.NUM_DOG ND, CO.CL_NAME CN, PC.SUMMA_DOG SD, PC.DATE_BEGIN DB, PC.DATE_END DE, Date_Otchot 
                                  FROM PR_CRED PC
                                  JOIN CLIENT CO
                                  ON PC.ID_CLIENT = CO.ID
                                  WHERE PC.DATE_BEGIN < Date_Otchot
                                  ORDER BY PC.DATE_BEGIN;
 Rec_Test_Otchot        Test_Otchot_1%ROWTYPE;
 Rec_Info_Test_Otchot   Test_Otchot_1%ROWTYPE;
 Rec_Cur_Cr_Cl          Cur_Credit_Client_Info%ROWTYPE;
 
 TYPE Nt_Sum_Oper IS TABLE OF FACT_OPER.F_SUMMA%TYPE;
 Nt_All_Credit          Nt_Sum_Oper;
 Nt_Paid_Credit         Nt_Sum_Oper;
 Nt_All_Percent         Nt_Sum_Oper;
 Nt_Paid_Percent        Nt_Sum_Oper;
 V_Rest_Credit          NUMBER (12,2);
 V_Rest_Percent         NUMBER (12,2);
 V_I                    NUMBER (10):= 1;
 BEGIN
        COMMIT; -- Для очистки временной таблицы Test_Otchot_1
        
        --Создание коллекции выданных кредитов на заданную дату Date_Otchot
        SELECT F_SUMMA BULK COLLECT INTO Nt_All_Credit FROM FACT_OPER
        WHERE F_DATE < Date_Otchot AND
              TYPE_OPER = 'Выдача кредита'
        ORDER BY COLLECTION_ID;
        
        --Создание коллекции погашенной части тела кредитов на заданную дату Date_Otchot  
        SELECT SUMMA BULK COLLECT INTO Nt_Paid_Credit FROM (SELECT COLLECTION_ID, SUM (F_SUMMA) SUMMA 
        FROM FACT_OPER
        WHERE F_DATE <= Date_Otchot AND
              TYPE_OPER = 'Погашение кредита'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID); 
        
        --Создание коллекции процентов по кредитам   
        SELECT SUMMA BULK COLLECT INTO Nt_All_Percent FROM (SELECT COLLECTION_ID, SUM (P_SUMMA) SUMMA 
        FROM PLAN_OPER
        WHERE TYPE_OPER = 'Погашение процентов'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID);
          
        --Создание коллекции погашенных процентов по кредитам на заданную дату Date_Otchot 
        SELECT SUMMA BULK COLLECT INTO Nt_Paid_Percent FROM (SELECT COLLECTION_ID, SUM (F_SUMMA) SUMMA 
        FROM FACT_OPER
        WHERE F_DATE <= Date_Otchot AND
              TYPE_OPER = 'Погашение процентов'
        GROUP BY COLLECTION_ID
        ORDER BY COLLECTION_ID);

        DBMS_OUTPUT.PUT_LINE (RPAD ('NUM_DOG', 15) || 
                              RPAD ('CL_NAME', 40) || 
                              RPAD ('SUM_DOG', 15) || 
                              RPAD ('DATE_BEGIN', 15) ||
                              RPAD ('DATE_END', 15) || 
                              RPAD ('REST_CREDIT', 15) ||
                              RPAD ('REST_PERCENT', 15) ||
                              RPAD ('Otchot_Date', 15));

        OPEN Cur_Credit_Client_Info;
        
        LOOP
        FETCH Cur_Credit_Client_Info INTO Rec_Cur_Cr_Cl;
        EXIT WHEN Cur_Credit_Client_Info%NOTFOUND;
        
        --Определение остатка ссудной задолженности
        IF V_I <= Nt_Paid_Credit.LAST THEN
              V_Rest_Credit := Nt_All_Credit (V_I) - Nt_Paid_Credit (V_I);
        ELSE  V_Rest_Credit := Nt_All_Credit (V_I);
        END IF;
        
        --Определение суммы предстоящих процентов
        IF V_I <= Nt_Paid_Percent.LAST THEN
              V_Rest_Percent := Nt_All_Percent (V_I) - Nt_Paid_Percent (V_I);
        ELSE  V_Rest_Percent := Nt_All_Percent (V_I);
        END IF;
        
        --Формирование записи отчёта, вставка во временную таблицу Test_Otchot_1 и вывод на экран вставленной строки
        Rec_Test_Otchot.NUM_DOG      := Rec_Cur_Cr_Cl.ND;
        Rec_Test_Otchot.CL_NAME      := Rec_Cur_Cr_Cl.CN;
        Rec_Test_Otchot.SUMMA_DOG    := Rec_Cur_Cr_Cl.SD;
        Rec_Test_Otchot.DATE_BEGIN   := Rec_Cur_Cr_Cl.DB;
        Rec_Test_Otchot.DATE_END     := Rec_Cur_Cr_Cl.DE;
        Rec_Test_Otchot.REST_CREDIT  := V_Rest_Credit; 
        Rec_Test_Otchot.REST_PERCENT := V_Rest_Percent;
        Rec_Test_Otchot.Otchot_Date  := Rec_Cur_Cr_Cl.Date_Otchot;
        
        INSERT INTO Test_Otchot_1 VALUES Rec_Test_Otchot RETURNING 
        NUM_DOG, CL_NAME, SUMMA_DOG, DATE_BEGIN, DATE_END, REST_CREDIT, REST_PERCENT, Otchot_Date 
        INTO Rec_Info_Test_Otchot;
        
        DBMS_OUTPUT.PUT_LINE (RPAD (Rec_Info_Test_Otchot.NUM_DOG, 15) ||
                              RPAD (Rec_Info_Test_Otchot.CL_NAME, 40) || 
                              RPAD (Rec_Info_Test_Otchot.SUMMA_DOG, 15) ||
                              RPAD (Rec_Info_Test_Otchot.DATE_BEGIN, 15) || 
                              RPAD (Rec_Info_Test_Otchot.DATE_END, 15) || 
                              RPAD (Rec_Info_Test_Otchot.REST_CREDIT, 15) ||
                              RPAD (Rec_Info_Test_Otchot.REST_PERCENT, 15) ||
                              RPAD (Rec_Info_Test_Otchot.Otchot_Date, 15));
        --Счетчик для индекса коллекций
        V_I := V_I + 1 ;
        END LOOP;
        
        CLOSE Cur_Credit_Client_Info; 
 END;

BEGIN
Proc_Test_Otchot ('30-10-2020');
END;
SELECT * FROM Test_Otchot_1;
 


 --Создание процедуры "Oткрытие кредитного договора"
 CREATE OR REPLACE PROCEDURE OPEN_DOGOVOR (Number_Dog    IN PR_CRED.NUM_DOG%TYPE,
                                           Sum_Dog       IN VARCHAR2,
                                           Date_Open     IN PR_CRED.DATE_BEGIN%TYPE,
                                           Month_Count   IN VARCHAR2)
 IS
 --Раздел объявлений для пользовательских исключений
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
 
 --Раздел объявлений для вставки строки Открытие нового договора в таблицу PR_CRED
 V_ID               PR_CRED.ID%TYPE;
 V_Id_Client        PR_CRED.ID_CLIENT%TYPE;
 V_Collect_Plan     PR_CRED.COLLECT_PLAN%TYPE;
 V_Collect_Fact     PR_CRED.COLLECT_FACT%TYPE;
 Add_Rec_Open_Dog   PR_CRED%ROWTYPE;
 Info_Opened_Dog    PR_CRED%ROWTYPE;
 
  --Раздел объявлений для вставки графика плановых операций для нового договора в таблицу PLAN_OPER
 V_P_Date           PLAN_OPER.P_DATE%TYPE;
 V_Percent_Credit   NUMBER;--PLAN_OPER.P_SUMMA%TYPE;
 V_Paid_Credit      NUMBER (12,2) := 0;
 V_Body_Credit      NUMBER (12,2) := 0;
 V_Amount_Payment   NUMBER;
 V_Percent_Rate     NUMBER;
 Add_Rec_Plan_Oper  PLAN_OPER%ROWTYPE;
 Info_Add_Rec_Plan  PLAN_OPER%ROWTYPE;
 
 BEGIN
       --Проверка введенных параметров процедуры Sum_Dog и Month_Count на соответствие определенному числовому типу данных и вызов исключения в случае несоответствия
       V_Sum_Dog := TRUNC (TO_NUMBER (Sum_Dog));
       V_Month_Count := TRUNC (TO_NUMBER (Month_Count));
       
       --Поиск исключений на основе введенных данных согласно бизнес-логике текущих потребительских кредитов Сбербанка
       
       --Определение даты открытия последнего договора
       SELECT MAX (DATE_BEGIN) INTO Min_Date FROM PR_CRED;
       
       --Создание коллекции существующих в системе договоров
       SELECT NUM_DOG BULK COLLECT INTO Nt_of_Exception FROM PR_CRED ORDER BY DATE_BEGIN;
       
       CASE
       WHEN REGEXP_SUBSTR (Number_Dog, '2021/\d+') IS NULL THEN
       RAISE Num_Dog_Exc;                         -- Вызов исключения при некорректном вводе номера договора
       
       WHEN Number_Dog MEMBER OF Nt_of_Exception THEN
       RAISE Unique_Constraint;                   -- Вызов исключения при наличии в системе заданного договора
       
       WHEN V_Sum_Dog  <  30000 OR V_Sum_Dog > 5000000 THEN
       RAISE Range_Summa_Exc;                     -- Вызов исключения при выходе за пределы допустимых значений суммы кредита для заданного договора

       WHEN Date_Open  < Min_Date THEN
       RAISE Min_Date_Exc;                        -- Вызов исключения, если введенная дата меньше даты последнего открытого договора 
       
       WHEN V_Month_Count  <  3 OR V_Month_Count > 60 THEN
       RAISE Range_Month_Exc;                     -- Вызов исключения при выходе за пределы допустимых значений срока кредита для заданного договора
       ELSE NULL;
       END CASE;
       
        --Назначение новому договору идентификатора
       SELECT MAX (ID) + 1111 INTO V_ID FROM PR_CRED;
        
       --Выбор клиента с наименьшей совокупной суммой кредитных договоров, которому открывается договор
       SELECT ID_CLIENT INTO V_Id_Client 
       FROM (SELECT ID_CLIENT, SUM (SUMMA_DOG) SUM_ALL_DOG 
             FROM PR_CRED 
             GROUP BY ID_CLIENT 
            ORDER BY SUM_ALL_DOG)
       WHERE rownum = 1;
       
        --Назначение новому договору идентификатора плановых операций
       SELECT MAX (COLLECT_PLAN) + 111 INTO V_Collect_Plan FROM PR_CRED;
        
       --Назначение новому договору идентификатора фактических операций
       V_Collect_Fact := V_Collect_Plan - 3;
       
       --Заполнение записи Add_Rec_Open_Dog необходимыми значениями
       Add_Rec_Open_Dog.ID            := V_ID;
       Add_Rec_Open_Dog.NUM_DOG       := Number_Dog;
       Add_Rec_Open_Dog.SUMMA_DOG     := V_Sum_Dog;
       Add_Rec_Open_Dog.DATE_BEGIN    := Date_Open;
       Add_Rec_Open_Dog.DATE_END      := ADD_MONTHS (Date_Open, V_Month_Count);
       Add_Rec_Open_Dog.ID_CLIENT     := V_Id_Client;
       Add_Rec_Open_Dog.COLLECT_PLAN  := V_Collect_Plan;
       Add_Rec_Open_Dog.COLLECT_FACT  := V_Collect_Fact;
       
       --Вставка строки для нового договора в таблицу PR_CRED и вывод на экран некоторых ее столбцов
       INSERT INTO PR_CRED VALUES Add_Rec_Open_Dog 
       RETURNING ID, NUM_DOG, SUMMA_DOG, DATE_BEGIN, DATE_END, ID_CLIENT, COLLECT_PLAN, COLLECT_FACT INTO Info_Opened_Dog;
       DBMS_OUTPUT.PUT_LINE (RPAD ('NEW_NUM_DOG', 20) || RPAD ('SUMMA_DOG', 20) || RPAD ('DATE_BEGIN', 20) || RPAD ('DATE_END', 20));
       DBMS_OUTPUT.PUT_LINE (RPAD (Info_Opened_Dog.NUM_DOG, 20) || RPAD (Info_Opened_Dog.SUMMA_DOG, 20) || 
                                  RPAD (Info_Opened_Dog.DATE_BEGIN, 20) || RPAD (Info_Opened_Dog.DATE_END, 20));
       DBMS_OUTPUT.PUT_LINE('--------------------------------------------------------------------');
       
       --Добавление операции 'Выдача кредита' по новому договору в таблицу таблицу плановых операций PLAN_OPER и вывод на экран вставленной строки
       Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
       Add_Rec_Plan_Oper.P_DATE         := Date_Open;
       Add_Rec_Plan_Oper.P_SUMMA        := V_Sum_Dog;
       Add_Rec_Plan_Oper.TYPE_OPER      := 'Выдача кредита';
       
       INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
       RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
       DBMS_OUTPUT.PUT_LINE (RPAD ('P_DATE', 20) || RPAD ('P_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
       DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
       DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------');
       
       --Определение процентной ставки по потребительскому кредиту в зависимости от суммы договора и срока кредита
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
       
       --Присвоение переменной значения функции расчета величины аннуитетного платежа, чтобы не вызывать функцию многократно в цикле
       V_Amount_Payment :=  Credit_Portfolio.Fun_Amount_Annuity_Payment (V_Sum_Dog, V_Percent_Rate, V_Month_Count);
       V_P_Date := Date_Open;
       
       FOR i IN 1..V_Month_Count
       LOOP
           --Определение текущего месяца
           V_P_Date := ADD_MONTHS (V_P_Date, 1);
           
           --Определение размера процентов в текущем месяце
           V_Percent_Credit := ROUND ((V_Sum_Dog - V_Body_Credit) * (V_Percent_Rate / 100 / 12), 2);
           
           --Добавление операции 'Погашение процентов' в текущем месяце по новому договору в таблицу плановых операций PLAN_OPER и вывод на экран вставленных строк
           Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
           Add_Rec_Plan_Oper.P_DATE         := V_P_Date;
           Add_Rec_Plan_Oper.P_SUMMA        := V_Percent_Credit;
           Add_Rec_Plan_Oper.TYPE_OPER      := 'Погашение процентов';
           
           INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
           RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
           DBMS_OUTPUT.PUT_LINE (RPAD ('P_DATE', 20) || RPAD ('P_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
           DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
                                   
           --Определение суммы погашения тела кредита из величины аннуитетного платежа
           V_Paid_Credit := V_Amount_Payment - V_Percent_Credit;
           
           --Добавление операции 'Погашение кредита' в текущем месяце по новому договору в таблицу плановых операций PLAN_OPER и вывод на экран вставленных строк
           Add_Rec_Plan_Oper.COLLECTION_ID  := V_Collect_Plan;
           Add_Rec_Plan_Oper.P_DATE         := V_P_Date;
           Add_Rec_Plan_Oper.P_SUMMA        := V_Paid_Credit;
           Add_Rec_Plan_Oper.TYPE_OPER      := 'Погашение кредита';
           
           INSERT INTO PLAN_OPER VALUES Add_Rec_Plan_Oper 
           RETURNING COLLECTION_ID, P_DATE, P_SUMMA, TYPE_OPER INTO Info_Add_Rec_Plan;
           DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Plan.P_DATE, 20) || RPAD (Info_Add_Rec_Plan.P_SUMMA, 20) || 
                                   RPAD (Info_Add_Rec_Plan.TYPE_OPER, 20));
           
           --Определение размера погашенного тела кредита и вывод на экран
           V_Body_Credit := V_Body_Credit + V_Paid_Credit;
           DBMS_OUTPUT.PUT_LINE ('Размер погашенного тела кредита = '||ROUND (V_Body_Credit, 1));
           DBMS_OUTPUT.PUT_LINE('-----------------------------------------------------------');
       END LOOP;
       DBMS_OUTPUT.PUT_LINE ('Открыт в системе новый кредитный договор № "' || Number_Dog || '" на сумму: ' || V_Sum_Dog ||
                             CHR (10)||'сроком на ' || V_Month_Count || ' месяцев и процентной ставкой - '||V_Percent_Rate||'%'||
                             CHR (10)||'тип платежа - аннуитетный, размер ежемесячного платежа - '||V_Amount_Payment||' руб.');
  
  EXCEPTION
           WHEN VALUE_ERROR THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка преобразования типа! Тип второго и четвертого параметров процедуры - NUMBER(10) и NUMBER(3)'||
                                  CHR (10)|| 'Тип третьего параметра - DATE в формате: '||CHR (39)||'ДД-ММ-ГГГГ'||CHR (39));
           --Обработка исключения недопустимого формата номера договора
           WHEN Num_Dog_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка вводимого номера договора! № "' || Number_Dog || '"- недопустимый формат номера'|| CHR (10)||
                               'Введите номер договора в формате: '|| CHR (39)||'2021/n'||CHR (39)||' , где n - число');
           GOTO end_of_proc; 
           
           --Обработка исключения уникальности номера договора и вывод на экран список существующих в системе
           WHEN Unique_Constraint THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка! Ограничение уникальности! Договор ' ||Number_Dog|| ' уже существует'||
                                  CHR (10)|| 'Существующие в системе номера договоров:');
           FOR i IN  Nt_of_Exception.FIRST..Nt_of_Exception.LAST
             LOOP  
             DBMS_OUTPUT.PUT_LINE (Nt_of_Exception (i));
             END LOOP;
           GOTO end_of_proc;
           
           --Обработка исключения выхода за пределы допустимых значений суммы кредита
           WHEN Range_Summa_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка суммы договора! Невозможно открыть кредитный договор на сумму: '||V_Sum_Dog||
                                CHR (10)|| 'Диапазон суммы кредита: от 30 000 до 5 000 000');
           GOTO end_of_proc;
           
           --Обработка исключения минимальной даты открываемого договора
           WHEN Min_Date_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка даты! Невозможно открыть договор датой, предшествующей дате последнего договора'||
                                CHR (10)|| 'Дата последнего открытого договора: '||Min_Date);
           GOTO end_of_proc;
           
           --Обработка исключения выхода за пределы допустимых значений срока кредита
           WHEN Range_Month_Exc THEN
           DBMS_OUTPUT.PUT_LINE ('Ошибка задания срока потребительского кредита! Диапазон допустимых значений: от 3 (3 месяца) до 60 (5 лет)');
           GOTO end_of_proc;
  RAISE; 
  --Метка завершения процедуры при вызове пользовательского исключения
  <<end_of_proc>>
  NULL;
 END;
       
 BEGIN
       OPEN_DOGOVOR ('2021/25', 300000, '25-02-2021', 6);
 END;
 
 SELECT * FROM PR_CRED;
 SELECT * FROM PLAN_OPER;
 
 --Удаление двух последних созданных договора  
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
 
 --Создание процедуры "Добавление операции 'Выдача кредита' " в таблицу FACT_OPER
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
      --Присвоение записи значений строки курсора
      OPEN  Cur_row_PR_CRED; 
      FETCH Cur_row_PR_CRED INTO Rec_Cur;
      CLOSE Cur_row_PR_CRED;
      
      IF Rec_Cur.COLLECT_FACT IS NULL THEN
      RAISE NO_DATA_FOUND;               -- Вызов исключения при отсутствии заданного договора
      END IF;
      
      --Создание коллекции договоров, по которым ещё не произведена операция "Выдача кредита"
      SELECT PC.NUM_DOG BULK COLLECT INTO Nt_Valid_NUM_DOG
      FROM PR_CRED PC
      LEFT JOIN FACT_OPER FO
      ON PC.COLLECT_FACT = FO.COLLECTION_ID
      WHERE FO.COLLECTION_ID IS NULL; 
      
      --Проверка на наличие уже выданного кредита для заданного договора
      IF Number_Dog NOT MEMBER OF Nt_Valid_NUM_DOG THEN
      RAISE Credit_Already_Issued;               -- Вызов исключения при наличии в системе выданного кредита для заданного договора
      END IF;

      --Присвоение записи необходимых значений и вставка в таблицу фактических операций FACT_OPER с выводом на экран вставленной строки
      Add_Rec_Fact_Oper.COLLECTION_ID := Rec_Cur.COLLECT_FACT;
      Add_Rec_Fact_Oper.F_DATE := Rec_Cur.DATE_BEGIN;
      Add_Rec_Fact_Oper.F_SUMMA := Rec_Cur.SUMMA_DOG;
      Add_Rec_Fact_Oper.TYPE_OPER := 'Выдача кредита';
      
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD ('COLLECTION_ID', 20) || RPAD ('F_DATE', 20) || RPAD ('F_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20));
 EXCEPTION
    -- Обработка исключения, если введенного номера договора не существует или введен некорректный формат номера
    WHEN NO_DATA_FOUND THEN
         DBMS_OUTPUT.PUT_LINE ('Ошибка! Выбранного договора № "' || Number_Dog || '" не существует в системе'|| CHR (10)||
                                   'Введите номер договора в формате: '|| CHR (39)||'2021/n'||CHR (39)||' , где n - число');
                   
    --Обработка исключения, если по данному договору уже выдан кредит
    WHEN Credit_Already_Issued THEN
        --Вывод на экран пользователю инфрмации о доступных договорах для проведения операции "Выдача кредита"
        DBMS_OUTPUT.PUT_LINE ('Ошибка! По выбранному договору № "' || Number_Dog || '" уже произведена операция "Выдача кредита"');
        IF Nt_Valid_NUM_DOG IS EMPTY THEN
           DBMS_OUTPUT.PUT_LINE ('В системе нет доступных договоров для проведения операции "Выдача кредита"'||
                                  CHR (10)|| 'Откройте в системе новый договор процедурой OPEN_DOGOVOR');
        ELSE 
             DBMS_OUTPUT.PUT_LINE ('Доступные договора для проведения операции "Выдача кредита":');
             FOR i IN  Nt_Valid_NUM_DOG.FIRST..Nt_Valid_NUM_DOG.LAST
             LOOP  
             DBMS_OUTPUT.PUT_LINE (Nt_Valid_NUM_DOG (i));
             END LOOP; 
         END IF;
         GOTO end_of_proc;
    RAISE;
    --Метка завершения процедуры при вызове пользовательского исключения
    <<end_of_proc>>
    NULL;
 END;
 
BEGIN
      Add_Issuance_Credit ('2021/10');
END;

SELECT * FROM FACT_OPER;

--Удаление созданной выше операции
DELETE FACT_OPER
WHERE COLLECTION_ID = 6446261210039;

--Создание процедуры "Добавление операции 'Погашение кредита' " в таблицу FACT_OPER
CREATE OR REPLACE PROCEDURE Add_Paid_Credit (Number_Dog IN PR_CRED.NUM_DOG%TYPE,
                                             Summa IN VARCHAR2)
 IS
   V_Summa            NUMBER (12,2);
   V_Collect_Plan     PR_CRED.COLLECT_PLAN%TYPE;
   V_Collect_Fact     PR_CRED.COLLECT_FACT%TYPE;
   V_F_Date           FACT_OPER.F_DATE%TYPE;
   TYPE Nt_of_Col_Id  IS TABLE OF FACT_OPER.COLLECTION_ID%TYPE;
   Nt_Close_Col_Id    Nt_of_Col_Id; -- Коллекция закрытых (погашенных) кредитов
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
      --Проверка введенного параметра процедуры Summa на числовой тип данных и вызов исключения, если Summa является строкой
      V_Summa := TO_NUMBER (Summa);
      
      --Определение COLLECTION_ID для заданного договора и вызов исключения, если договор не найден
      SELECT COLLECT_PLAN, COLLECT_FACT  INTO V_Collect_Plan, V_Collect_Fact FROM PR_CRED
      WHERE NUM_DOG = Number_Dog;
      
      --Определение размера выданного кредита по заданному договору и вызов исключения, если по этому договору ещё не выдан кредит
      SELECT F_SUMMA INTO V_Ostatok_Credit FROM FACT_OPER
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = 'Выдача кредита';
            
      --Проверка на наличие уже погашенного кредита по введенному договору
      SELECT COLLECTION_ID BULK COLLECT INTO Nt_Close_Col_Id FROM FACT_OPER
      WHERE TYPE_OPER = 'Кредит погашен'; 
      
      IF V_Collect_Fact MEMBER OF Nt_Close_Col_Id THEN
      RAISE Credit_Close_Exc;               -- Вызов исключения при наличии в системе уже погашенного (закрытого) кредита для заданного договора
      END IF;
      
      --Определение F_Date для заданного договора
      SELECT  MIN (PO.P_DATE) INTO V_F_Date
      FROM      PLAN_OPER PO
      LEFT JOIN FACT_OPER FO
      ON PO.P_DATE = FO.F_DATE
      WHERE PO.COLLECTION_ID = V_Collect_Plan AND
            PO.TYPE_OPER = 'Погашение кредита' AND
            (PO.P_DATE IS NULL OR FO.F_DATE IS NULL);
      
      --Определение P_SUMMA для планового погашения тела кредита
      SELECT P_SUMMA INTO V_Paid_Credit FROM PLAN_OPER
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = 'Погашение кредита' AND
            P_DATE = V_F_Date;     
      
      --Определение F_SUMMA для погашения процентов      
      SELECT P_SUMMA INTO V_Percent_Credit FROM PLAN_OPER
      WHERE COLLECTION_ID = V_Collect_Plan AND
            TYPE_OPER = 'Погашение процентов' AND
            P_DATE = V_F_Date;     
      
      -- Проверка введенной Summa на минимальное значение платежа в текущем месяце для заданного договора
      IF    V_Summa != 1 AND V_Summa != 2 AND  V_Summa  <  (V_Paid_Credit + V_Percent_Credit) THEN
      RAISE Min_Summa_Exc;   -- Вызов исключения, если введенная сумма меньше планового платежа в текущем месяце           
      END IF;
      
      --Добавление строки 'Погашение процентов' в таблицу FACT_OPER и вывод на экран вставленных строк для заданного договора
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Percent_Credit;
      Add_Rec_Fact_Oper.TYPE_OPER      := 'Погашение процентов';
      
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD ('COLLECTION_ID', 20) || RPAD ('F_DATE', 20) || RPAD ('F_SUMMA', 20) || RPAD ('TYPE_OPER', 20));
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20)); 
            
      --Определение размера погашенной части тела кредита
      SELECT SUM (F_SUMMA) INTO V_Path_Credit FROM FACT_OPER
      WHERE COLLECTION_ID = V_Collect_Fact AND
            TYPE_OPER = 'Погашение кредита';      
      
      --Определение F_SUMMA и других параметров в зависимости от введенной Summa       
      CASE
      --Если введено "1", то будет погашено тело кредита в размере ближайшего планового платежа V_Paid_Credit 
      WHEN V_Summa = 1 THEN
      --Определение остатка ссудной задолженности
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - V_Paid_Credit;
      --Если введено "2", то будет полностью погашено тело кредита 
      WHEN V_Summa = 2 THEN
           V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := 0;
      --Определение размера погашения тела кредита V_Paid_Credit и остатка ссудной задолженности, если введена произвольная Summa, не превышающая остатка ссудной задолженности
      WHEN (V_Ostatok_Credit - V_Path_Credit) > (V_Summa - V_Percent_Credit) THEN
           V_Ostatok_Credit := V_Ostatok_Credit - V_Path_Credit - (V_Summa - V_Percent_Credit);
           V_Paid_Credit := V_Summa - V_Percent_Credit;
      --Полное погашение тела кредита и определение излишне внесенных средств клиентом для последуюущего возврата при введенной Summa > остатка ссудной задолженности
      ELSE V_Paid_Credit := V_Ostatok_Credit - V_Path_Credit;
           V_Ostatok := V_Summa - V_Percent_Credit - V_Paid_Credit;
      END CASE;     
      
      --Добавление строки 'Погашение кредита' в таблицу FACT_OPER и вывод на экран вставленных строк для заданного договора
      Add_Rec_Fact_Oper.COLLECTION_ID  := V_Collect_Fact;
      Add_Rec_Fact_Oper.F_DATE         := V_F_Date;
      Add_Rec_Fact_Oper.F_SUMMA        := V_Paid_Credit;
      IF   V_Ostatok IS NULL THEN
           Add_Rec_Fact_Oper.TYPE_OPER      := 'Погашение кредита';
      ELSE
           Add_Rec_Fact_Oper.TYPE_OPER      := 'Кредит погашен';
      END IF;     
           
      INSERT INTO FACT_OPER VALUES Add_Rec_Fact_Oper 
      RETURNING COLLECTION_ID, F_DATE, F_SUMMA, TYPE_OPER INTO Info_Add_Rec_Fact;
      DBMS_OUTPUT.PUT_LINE (RPAD (Info_Add_Rec_Fact.COLLECTION_ID, 20) || RPAD (Info_Add_Rec_Fact.F_DATE, 20) ||
                            RPAD (Info_Add_Rec_Fact.F_SUMMA, 20) || RPAD (Info_Add_Rec_Fact.TYPE_OPER, 20));
      DBMS_OUTPUT.PUT_LINE('-------------------------------------------------------------------------------');
      --Вывод на экран информации о состоянии кредитного договора после операции "Погашение кредита"
      IF   V_Ostatok IS NULL THEN
           DBMS_OUTPUT.PUT_LINE ('Остаток по телу непогашенного кредита по договору '|| Number_Dog || ' составляет: '|| V_Ostatok_Credit);
      ELSE 
           DBMS_OUTPUT.PUT_LINE ('Кредит по договору '|| Number_Dog || ' полностью погашен');
           IF V_Ostatok != 0 THEN
           DBMS_OUTPUT.PUT_LINE ('Излишне внесенные клиентом деньги в размере '||V_Ostatok||' переведены на его расчётный счет');
           END IF;                 
      END IF;
 EXCEPTION
           -- Обработка исключения, если введенного номера договора не существует или введен некорректный формат номера
           WHEN VALUE_ERROR THEN
                DBMS_OUTPUT.PUT_LINE ('Ошибка ORA-06502! VALUE_ERROR!' || CHR (10)||
                                      'Второй параметр процедуры является числом, введите число с точностью не более 2-х знаков после запятой');
           WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.PUT_LINE ('Ошибка! Выбранного договора № "' || Number_Dog || '" не существует в системе или по договору ещё не выдан кредит'|| CHR (10)||
                               'Введите номер договора в формате: '|| CHR (39)||'2021/n'||CHR (39)||' , где n - число');
           
           WHEN Credit_Close_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('Ошибка! По выбранному договору № "' || Number_Dog || '" кредит уже полностью погашен ');
           GOTO end_of_proc; 
           
           WHEN Min_Summa_Exc THEN
                DBMS_OUTPUT.PUT_LINE ('Ошибка введенной суммы погашения!' || CHR (10)|| 
                                 'Введенная сумма ' || Summa || ' не должна быть меньше величины обязательного платежа в текущем месяце'|| CHR (10)||
                                 'Обязательный платеж в текущем месяце для договора '|| Number_Dog ||' составляет: '|| (V_Paid_Credit + V_Percent_Credit));
           GOTO end_of_proc;                     
  RAISE; 
  --Метка завершения процедуры при вызове пользовательского исключения
  <<end_of_proc>>
  NULL;
 END;

BEGIN
      Add_Paid_Credit ('2021/1', 30000);
END;


--Удаление созданной выше операции
DELETE FACT_OPER
WHERE COLLECTION_ID = 6120475936893 AND
      F_DATE = '03-03-2021';
           












