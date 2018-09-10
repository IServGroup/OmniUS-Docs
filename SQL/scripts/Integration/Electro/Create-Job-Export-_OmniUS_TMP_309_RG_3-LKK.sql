BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'Data Collector' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'Data Collector'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Отправка данных в ЛКК ЮЛ (РусГидро, ЧЭСК)', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@category_name=N'Data Collector', 
		@owner_login_name=N'compulink\a-lytchev', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка ЛС]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка ЛС', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- ЛС
SET @success = 0;
SET @msg = NULL;
SET @count = 0;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	-- данные по ЛС, еще отсутствующим в буфере
	INSERT INTO [IE].LKK_subscrs
		(link, n_code, c_name, c_name_short, c_address, c_post_address, d_date_begin, d_date_end,
		n_contract, c_contract, d_contract_date, d_contract_begin, d_contract_end,
		c_kpp, c_inn, c_bank_name, c_bank_bik, c_bank_inn, c_bank_rs,
		c_phone, c_email, c_fax, c_site,
		b_ee)
	SELECT
		ss.LINK						AS link,
		ss.N_Code					AS n_code,
		CASE WHEN cpt.C_Const = ''PAR_Person''
			THEN cp.C_Name1 + ISNULL('' '' + cp.C_Name2, '''') + ISNULL('' '' + cp.C_Name3, '''')
			ELSE cp.C_Name1
		END							AS c_name,
		CASE WHEN cpt.C_Const = ''PAR_Person''
			THEN cp.C_Name1 + ISNULL('' '' + LEFT(cp.C_Name2, 1) + ''.'', '''') + ISNULL('' '' + LEFT(cp.C_Name3, 1) + ''.'', '''')
			ELSE cp.C_Name2
		END							AS c_name_short,
		cp.C_Address1				AS c_address,
		cp.C_Address2				AS c_post_address,
		ss.D_Date_Begin				AS d_date_begin,
		ss.D_Date_End				AS d_date_end,
		contr.link					AS n_contract,
		LEFT(contr.C_Number, 24)	AS c_contract,
		contr.D_Date				AS d_contract_date,
		contr.D_Date_Begin			AS d_contract_begin,
		contr.D_Date_End			AS d_contract_end,
		cp.N_KPP					AS c_kpp,
		LEFT(cp.N_INN, 12)			AS c_inn,
		acc.C_Bank					AS c_bank_name,
		acc.C_Bank_BIK				AS c_bank_bik,
		acc.C_Bank_INN				AS c_bank_inn,
		acc.C_Bank_RS				AS c_bank_rs,
		LEFT(cp.C_Telephone, 128)	AS c_phone,
		cp.C_Email					AS c_email,
		cp.C_Fax					AS c_fax,
		cp.C_URL					AS c_site,
		ss.B_EE						AS b_ee
	FROM dbo.SD_Subscr AS ss
	INNER JOIN dbo.CD_Partners AS cp
		ON cp.LINK = ss.F_Partners
	INNER JOIN dbo.CS_Partner_Types AS cpt
		ON cpt.LINK = cp.F_Partner_Types
	-- последний по дате заключения договор (у типа документа B_Contract=1), сначала из незакрытых
	LEFT JOIN (
		SELECT
			dd.link,
			dd.F_Subscr,
			ROW_NUMBER() OVER(PARTITION BY dd.F_Subscr ORDER BY dd.B_Done, dd.D_Date_Begin DESC) AS N,
			dd.D_Date,
			dd.C_Number,
			dd.D_Date_Begin,
			dd.D_Date_End,
			dd.S_Create_Date,
			dd.S_Modif_Date
		FROM dbo.DD_Docs AS dd
		INNER JOIN dbo.DS_Docum_Types AS ddt
			ON ddt.LINK = dd.F_Docum_Types
		WHERE ddt.B_Contract = 1
	) contr
		ON contr.F_Subscr = ss.LINK
		AND contr.N = 1
	-- последний по дате открытия р/с, сначала из счетов по умолчанию
	LEFT JOIN (
		SELECT
			fba.F_Partners,
			ROW_NUMBER() OVER(PARTITION BY fba.F_Partners ORDER BY CASE WHEN fba.B_Default = 1 THEN 1 ELSE 100 END, fba.D_Date_Begin DESC) AS N,
			fb.C_Name AS C_Bank,
			fb.N_BIC AS C_Bank_BIK,
			fb.N_INN AS C_Bank_INN,
			fba.N_Account AS C_Bank_RS,
			fba.S_Create_Date,
			fba.S_Modif_Date,
			fb.S_Create_Date AS S_Create_Date_Bank,
			fb.S_Modif_Date AS S_Modif_Date_Bank
		FROM dbo.FD_Bank_Accounts AS fba
		LEFT JOIN dbo.FS_Banks AS fb
			ON fb.LINK = fba.F_Banks
	) acc
		ON acc.F_Partners = ss.F_Partners
		AND acc.N = 1
	CROSS APPLY dbo.MF_DateMAX14(ss.S_Create_Date, ss.S_Modif_Date, cp.S_Create_Date, cp.S_Modif_Date, contr.S_Create_Date, contr.S_Modif_Date, acc.S_Create_Date, acc.S_Modif_Date, acc.S_Create_Date_Bank, acc.S_Modif_Date_Bank, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_subscrs s
		ON s.link = ss.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного ЛС в буфер - позже даты последнего изменения чего-то из данных ЛС
	WHERE ss.LINK > 0
		AND s.link IS NULL					-- если в буфере нет данного ЛС (причем туда он попал бы уже после всех изменений), то такой ЛС нам подойдет
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today);

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано ЛС: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке ЛС произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка ЛС отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''dbo.SD_Subscr'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка типов ПУ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка типов ПУ', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- Типы ПУ
SET @success = 0;
SET @msg = NULL;
SET @count = 0;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	-- данные по типам ПУ, еще отсутствующим в буфере
	INSERT INTO [IE].LKK_devices
		(id, c_energy_category, c_device_category, c_name, c_modification, n_check_cycle,
		n_tariff, b_phase3, c_manufacturer, c_precission_class, c_voltage_nominal)
	SELECT
		edt.LINK				AS id,
		eec.C_Name				AS c_energy_category,
		edc.C_Name				AS c_device_category,
		edt.C_Name				AS c_name,
		edt.C_Modification		AS c_modification,
		edt.N_Check_Cycle		AS n_check_cycle,
		edt.N_Tariff			AS n_tariff,
		edt.B_Phase3			AS b_phase3,
		edt.C_Producer_Factory	AS c_manufacturer,
		epc.C_Name				AS c_precission_class,
		edt.C_Voltage_Range		AS c_voltage_nominal
	FROM dbo.ES_Device_Types AS edt
	INNER JOIN dbo.ES_Device_Categories AS edc
		ON edc.LINK = edt.F_Device_Categories
	INNER JOIN dbo.ES_Energy_Category AS eec
		ON eec.LINK = edt.F_Energy_Category
	LEFT JOIN dbo.ES_Precission_Classes AS epc
		ON epc.LINK = edt.F_Precission_Class
	CROSS APPLY dbo.MF_DateMAX14(edt.S_Create_Date, edt.S_Modif_Date, edc.S_Create_Date, edc.S_Modif_Date, eec.S_Create_Date, eec.S_Modif_Date, epc.S_Create_Date, epc.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_devices s
		ON s.id = edt.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного типа ПУ в буфер - позже даты последнего изменения чего-то из данных типа ПУ
	WHERE s.id IS NULL;						-- если в буфере нет данного типа ПУ (причем туда он попал бы уже после всех изменений), то такой тип ПУ нам подойдет

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано типов ПУ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке типов ПУ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка типов ПУ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''dbo.ES_Device_Types'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка ПУ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка ПУ', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- ПУ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	-- данные по ПУ, еще отсутствующим в буфере
	INSERT INTO [IE].LKK_subscr_devices
		(link, n_subscr, f_device, c_name, c_number, n_year,
		d_setup, c_install_place, d_check, d_next_check,
		d_valid, d_replace_before, c_energy_lvl, n_rate, d_replace)
	SELECT
		ed.LINK					AS link,
		erp.F_Subscr			AS n_subscr,
		ed.F_Device_Types		AS f_device,
		erp.C_Name				AS c_name,
		ed.C_Serial_Number		AS c_number,
		ed.N_Manufacture_Year	AS n_year,
		ed.D_Setup_Date			AS d_setup,
		edl.C_Name + ISNULL('', '' + ISNULL(NULLIF(scp.C_Address, ''''), NULLIF(enp.C_Address, '''')), '''')
								AS c_install_place,
		ed.D_Valid_Date			AS d_check,
		DATEADD(yy, edt.N_Check_Cycle, ed.D_Valid_Date)
								AS d_next_check,
		ed.D_Valid_Date			AS d_valid,
		ed.D_Replace_Before		AS d_replace_before,
		eel.C_Name				AS c_energy_lvl,
		ed.N_Rate				AS n_rate,
		ed.D_Replace_Date		AS d_replace
	FROM dbo.ED_Devices AS ed
	INNER JOIN dbo.ED_Devices_Pts AS edp
		LEFT JOIN dbo.ED_Devices_Pts AS edp2
			ON edp2.F_Devices = edp.F_Devices
			AND edp2.F_Registr_Pts <> edp.F_Registr_Pts
		ON edp.F_Devices = ed.LINK
		AND (edp.B_Main = 1 OR edp2.LINK IS NULL)
	INNER JOIN dbo.ED_Registr_Pts AS erp
		ON erp.LINK = edp.F_Registr_Pts
	LEFT JOIN dbo.ED_Network_Pts AS enp
		ON enp.LINK = ISNULL(ed.F_Network_Pts, erp.F_Network_Pts)
	LEFT JOIN dbo.SD_Conn_Points AS scp
		ON scp.LINK = enp.F_Conn_Points
	INNER JOIN dbo.ES_Device_Types AS edt
		ON edt.LINK = ed.F_Device_Types
	LEFT JOIN dbo.ES_Device_Locations AS edl
		ON edl.LINK = ed.F_Device_Locations
	LEFT JOIN dbo.ES_Energy_Levels AS eel
		ON eel.LINK = erp.F_Energy_Levels
	CROSS APPLY dbo.MF_DateMAX14(ed.S_Create_Date, ed.S_Modif_Date, edp.S_Create_Date, edp.S_Modif_Date, erp.S_Create_Date, erp.S_Modif_Date, edt.S_Create_Date, edt.S_Modif_Date, edl.S_Create_Date, edl.S_Modif_Date, eel.S_Create_Date, eel.S_Modif_Date, enp.S_Create_Date, enp.S_Modif_Date) md
	LEFT JOIN [IE].LKK_subscr_devices s
		ON s.link = ed.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного ПУ в буфер - позже даты последнего изменения чего-то из данных ПУ
	WHERE s.link IS NULL					-- если в буфере нет данного ПУ (причем туда он попал бы уже после всех изменений), то такой ПУ нам подойдет
		-- неснятый ПУ
		--AND (ed.D_Replace_Date IS NULL OR ed.D_Replace_Date > @today)
		-- уже установленный ПУ
		AND (ed.D_Setup_Date < @today)
		-- на уже открытом УП
		AND (erp.D_Date_Begin < @today)
		-- на незакрытом УП
		AND (erp.D_Date_End IS NULL OR erp.D_Date_End > @today);

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано ПУ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке ПУ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка ПУ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''dbo.ED_Devices'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка показаний]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка показаний', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- Показания
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	-- данные по Показаниям, еще отсутствующим в буфере
	INSERT INTO [IE].LKK_meters
		(link, n_device, d_date_prev, d_date, n_value, n_cons, n_quantity,
		n_year, n_month, n_days, c_delivery_method, c_timezone, c_energytype, n_status, c_status)
	SELECT
		emr.LINK			AS link,
		emr.F_Devices		AS n_device,
		emr.D_Date_Prev		AS d_date_prev,
		emr.D_Date			AS d_date,
		emr.N_Value			AS n_value,
		emr.N_Cons			AS n_cons,
		emr.S_Quantity		AS n_quantity,
		emr.N_Year			AS n_year,
		emr.N_Month			AS n_month,
		emr.S_Days			AS n_days,
		edm.C_Name			AS c_delivery_method,
		ftz.C_Name			AS c_timezone,
		eet.C_Short_Name	AS c_energytype,
		ers.N_Code			AS n_status,
		ers.C_Name			AS c_status
	FROM dbo.ED_Meter_Readings AS emr
	INNER JOIN dbo.ES_Delivery_Methods AS edm
		ON edm.LINK = emr.F_Delivery_Methods
	INNER JOIN dbo.FS_Time_Zones AS ftz
		ON ftz.LINK = emr.F_Time_Zones
	INNER JOIN dbo.ES_Energy_Types AS eet
		ON eet.LINK = emr.F_Energy_Types
	INNER JOIN dbo.ES_Readings_Status AS ers
		ON ers.LINK = emr.F_Readings_Status
	INNER JOIN dbo.ED_Devices AS ed
		ON ed.LINK = emr.F_Devices
	CROSS APPLY dbo.MF_DateMAX14(emr.S_Create_Date, emr.S_Modif_Date, edm.S_Create_Date, edm.S_Modif_Date, ftz.S_Create_Date, ftz.S_Modif_Date, eet.S_Create_Date, eet.S_Modif_Date, ers.S_Create_Date, ers.S_Modif_Date, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_meters s
		ON s.link = emr.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного Показания в буфер - позже даты последнего изменения чего-то из данных Показания
	WHERE s.link IS NULL					-- если в буфере нет данного Показания (причем туда оно попало бы уже после всех изменений), то такое Показание нам подойдет
		-- неснятый ПУ
		--AND (ed.D_Replace_Date IS NULL OR ed.D_Replace_Date > @today)
		-- уже установленный ПУ
		AND (ed.D_Setup_Date < @today);

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано показаний: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке показаний произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка показаний отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''dbo.ED_Meter_Readings'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка сальдо по ЛС]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка сальдо по ЛС', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- Сальдо по ЛС
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	-- данные по ЛС+периоду, еще отсутствующим в буфере
	INSERT INTO [IE].LKK_saldo
		(n_subscr, d_date, n_amount, n_peny, n_persent, d_lastpayment_date, n_lastpayment_amount, c_lastpayment_link)
	-- ЛС ЮЛ
	SELECT
		ss.LINK						AS n_subscr,
		@d_today					AS d_date,
		-1 * SUM(fss.N_Amount1)		AS n_amount,
		-1 * SUM(CASE WHEN fsc.B_Penalty = 1 OR fsi.C_Const = ''SIT_Penalty'' THEN fss.N_Amount1 ELSE 0 END)
									AS n_peny,
		-1 * SUM(CASE WHEN fsi.C_Const = ''CRT_Percent_Money'' THEN fss.N_Amount1 ELSE 0 END)
									AS n_persent,
		MAX(pay.D_Date)				AS d_lastpayment_date,
		MAX(pay.N_Amount)			AS n_lastpayment_amount,
		MAX(pay.F_Last_Payment)		AS c_lastpayment_link
	FROM dbo.SD_Subscr AS ss
	CROSS APPLY dbo.CF_Period_Dates(@month, @year) AS cpd
	CROSS APPLY EE.FF_Saldo_Simple(ss.F_Division, cpd.D_Start, cpd.D_End, 1) AS fss
	INNER JOIN dbo.FS_Sale_Categories AS fsc
		ON fsc.LINK = fss.F_Sale_Categories
	INNER JOIN dbo.FS_Sale_Items AS fsi
		ON fsi.LINK = fss.F_Sale_Items
	LEFT JOIN (
			SELECT
				fp.F_Subscr,
				fp.D_Date,
				fp.N_Amount,
				fp.LINK AS F_Last_Payment,
				ROW_NUMBER() OVER(PARTITION BY fp.F_Subscr ORDER BY fp.D_Date DESC) AS N_Order
			FROM EE.FD_Payments AS fp
		) pay
			ON pay.F_Subscr = fss.F_Subscr
			AND pay.N_Order = 1
	LEFT JOIN [IE].LKK_saldo s
		ON s.n_subscr = ss.LINK
		AND s.d_date = @d_today
	WHERE s.id IS NULL						-- если в буфере нет сальдо на сегодня
		AND ss.B_EE = 1
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today)
	GROUP BY ss.LINK

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано записей о состоянии расчетов: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке состояния расчетов произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка состояния расчетов отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Totals'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка РВ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка РВ', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- РВ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY
	INSERT INTO [IE].LKK_paysheets
		(link, n_subscr, n_sale_category, c_sale_category, n_doctype, c_doctype,
		n_status, c_status, d_date, d_date_begin, d_date_end, c_number,
		n_period, n_amount, n_tax, n_quantity, n_quantity2, n_cons)
	SELECT
		fp.LINK						AS link,
		ss.LINK						AS n_subscr,
		fsc.N_Code					AS n_sale_category,
		fsc.C_Name					AS c_sale_category,
		fdt.N_Code					AS n_doctype,
		fdt.C_Name					AS c_doctype,
		fs.LINK						AS n_status,
		fs.C_Name					AS c_status,
		fp.D_Date					AS d_date,
		fp.D_Date_Begin				AS d_date_begin,
		fp.D_Date_End				AS d_date_end,
		fp.C_Number					AS c_number,
		fp.N_Period					AS n_period,
		fp.N_Amount					AS n_amount,
		fp.N_Tax_Amount				AS n_tax,
		fp.N_Quantity				AS n_quantity,
		fp.N_Quantity2				AS n_quantity2,
		fp.N_Cons					AS n_cons
	FROM dbo.SD_Subscr AS ss
	INNER JOIN EE.FD_Paysheets AS fp
		ON fp.F_Subscr = ss.LINK
	INNER JOIN dbo.FS_Sale_Categories AS fsc
		ON fsc.LINK = fp.F_Sale_Categories
	INNER JOIN dbo.FS_Doc_Types AS fdt
		ON fdt.LINK = fp.F_Doc_Types
	INNER JOIN dbo.FS_Debts AS fd
		ON fd.LINK = fp.F_Debts
	INNER JOIN dbo.FS_Status AS fs
		ON fs.LINK = fp.F_Status
	CROSS APPLY dbo.MF_DateMAX14(fp.S_Create_Date, fp.S_Modif_Date, fsc.S_Create_Date, fsc.S_Modif_Date, fdt.S_Create_Date, fdt.S_Modif_Date, fd.S_Create_Date, fd.S_Modif_Date, fs.S_Create_Date, fs.S_Modif_Date, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_paysheets s
		ON s.link = fp.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данной РВ в буфер - позже даты последнего изменения чего-то из данных РВ
	WHERE s.id IS NULL						-- если в буфере нет данной РВ (причем туда она попала бы уже после всех изменений), то такая РВ нам подойдет
		AND ss.B_EE = 1
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today)

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано РВ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке РВ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка РВ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Paysheets'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка строк РВ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка строк РВ', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- строки РВ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	INSERT INTO [IE].LKK_paysheets_details
		(link, n_paysheet, n_device, c_device, n_invoce_grp, n_bill_grp,
		d_date_begin, d_date_end, n_cons, n_quantity, n_tariff_amount,
		n_amount, n_tax, n_persent, n_mr_cons, n_period, c_calc_method,
		n_energy_type, c_energy_type, c_voltage_nominal, n_tariff, c_tariff,
		n_tariff_zone, c_tariff_zone, n_tax_persent, c_sale_item, c_sale_item_doc,
		n_unit, c_unit, n_energy_lvl, c_energy_lvl)
	SELECT
		fpd.LINK			AS link,
		fpd.F_Paysheets		AS n_paysheet,
		fpd.F_Devices		AS n_device,
		ed.C_Serial_Number	AS c_device,
		fpd.F_Inv_Grp		AS n_invoce_grp,
		fpd.F_Bill_Grp		AS n_bill_grp,
		fpd.D_Date_Begin	AS d_date_begin,
		fpd.D_Date_End		AS d_date_end,
		fpd.N_Cons			AS n_cons,
		fpd.N_Quantity		AS n_quantity,
		fpd.N_Tariff		AS n_tariff_amount,
		fpd.N_Amount		AS n_amount,
		fpd.N_Tax_Amount	AS n_tax,
		fpd.N_Percent		AS n_persent,
		fpd.N_MR_Cons		AS n_mr_cons,
		fpd.N_Period		AS n_period,
		ecm.C_Name			AS c_calc_method,
		eet.N_Code			AS n_energy_type,
		eet.C_Short_Name	AS c_energy_type,
		evn.C_Name			AS c_voltage_nominal,
		ISNULL(TRY_CONVERT(SMALLINT, ft.N_Code), 0)
							AS n_tariff,
		ft.C_Name			AS c_tariff,
		ftz.N_Code			AS n_tariff_zone,
		ftz.C_Name			AS c_tariff_zone,
		fpd.N_Tax			AS n_tax_persent,
		fsi.C_Name			AS c_sale_item,
		fsi.C_Doc_Name		AS c_sale_item_doc,
		fu.N_Code			AS n_unit,
		fu.C_Name			AS c_unit,
		eel.N_Code			AS n_energy_lvl,
		eel.C_Name			AS c_energy_lvl
	FROM [IE].LKK_paysheets fp
	INNER JOIN EE.FVT_Paysheets_Details AS fpd
		ON fpd.F_Paysheets = fp.link
	LEFT JOIN dbo.ED_Devices AS ed
		ON ed.LINK = fpd.F_Devices
	LEFT JOIN dbo.FS_Sub_Operation_Types AS fsot
		ON fsot.LINK = fpd.F_Sub_Operation_Types
	INNER JOIN dbo.ES_Calc_Methods AS ecm
		ON ecm.LINK = fpd.F_Calc_Methods
	LEFT JOIN dbo.ES_Energy_Types AS eet
		ON eet.LINK = fpd.F_Energy_Types
	LEFT JOIN dbo.FS_Time_Zones AS ftz
		ON ftz.LINK = fpd.F_Time_Zones
	LEFT JOIN dbo.FS_Tariff AS ft
		ON ft.LINK = fpd.F_Tariff
	LEFT JOIN dbo.FS_Units AS fu
		ON fu.LINK = fpd.F_Units
	LEFT JOIN dbo.FS_Sale_Items AS fsi
		ON fsi.LINK = fpd.F_Sale_Items
	LEFT JOIN dbo.ES_Voltage_Nominal AS evn
		ON evn.LINK = fpd.F_Voltage_Nominal
	LEFT JOIN dbo.ES_Energy_Levels AS eel
		ON eel.LINK = fpd.F_Energy_Levels
	CROSS APPLY dbo.MF_DateMAX14(fpd.S_Create_Date, fpd.S_Modif_Date, ed.S_Create_Date, ed.S_Modif_Date, fsot.S_Create_Date, fsot.S_Modif_Date, ftz.S_Create_Date, ftz.S_Modif_Date, ft.S_Create_Date, ft.S_Modif_Date, fu.S_Create_Date, fu.S_Modif_Date, fsi.S_Create_Date, fsi.S_Modif_Date) md
	LEFT JOIN [IE].LKK_paysheets_details s
		ON s.link = fpd.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данной Строки РВ в буфер - позже даты последнего изменения чего-то из данных строки
	WHERE s.id IS NULL						-- если в буфере нет данной Строки РВ (причем туда он попал бы уже после всех изменений), то такая Строка РВ нам подойдет

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано строк РВ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке строк РВ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка строк РВ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Paysheet_Details'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка СчФ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка СчФ', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- СчФ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY
	INSERT INTO [IE].LKK_invoices
		(link,
		n_subscr,
		n_sale_category,
		c_sale_category,
		n_doc_type,
		c_doc_type,
		n_paysheet,
		n_payment,
		n_debts,
		c_debts,
		n_status,
		c_status,
		d_date,
		d_date_begin,
		d_date_end,
		d_date_due,
		c_number,
		n_period,
		n_amount,
		n_tax,
		n_quantity,
		n_quantity2,
		n_cons,
		b_debit,
		b_closed,
		n_unit,
		c_unit)
	SELECT
		fi.LINK						AS link,
		ss.LINK						AS n_subscr,
		fsc.N_Code					AS n_sale_category,
		fsc.C_Name					AS c_sale_category,
		fdt.N_Code					AS n_doc_type,
		fdt.C_Name					AS c_doc_type,
		fi.F_Paysheets				AS n_paysheet,
		fi.F_Payments				AS n_payment,
		fd.N_Code					AS n_debts,
		fd.C_Name					AS c_debts,
		fs.LINK						AS n_status,
		fs.C_Name					AS c_status,
		fi.D_Date					AS d_date,
		fi.D_Date_Begin				AS d_date_begin,
		fi.D_Date_End				AS d_date_end,
		fi.D_Date_Due				AS d_date_due,
		fi.C_Number					AS c_number,
		fi.N_Period					AS n_period,
		fi.N_Amount					AS n_amount,
		fi.N_Tax_Amount				AS n_tax,
		fi.N_Quantity				AS n_quantity,
		fi.N_Quantity2				AS n_quantity2,
		fi.N_Cons					AS n_cons,
		fi.B_Debit					AS b_debit,
		fi.B_Closed					AS b_closed,
		units.N_Code				AS n_unit,
		units.C_Name				AS c_unit
	FROM dbo.SD_Subscr AS ss
	INNER JOIN EE.FD_Invoices AS fi
		ON fi.F_Subscr = ss.LINK
	INNER JOIN dbo.FS_Sale_Categories AS fsc
		ON fsc.LINK = fi.F_Sale_Categories
	INNER JOIN dbo.FS_Doc_Types AS fdt
		ON fdt.LINK = fi.F_Doc_Types
	INNER JOIN dbo.FS_Debts AS fd
		ON fd.LINK = fi.F_Debts
	INNER JOIN dbo.FS_Status AS fs
		ON fs.LINK = fi.F_Status
	LEFT JOIN (
		SELECT
			fid.F_Invoices, fu.N_Code, fu.C_Name
		FROM EE.FVT_Invoices_Details AS fid
		INNER JOIN dbo.FS_Units AS fu
			ON fu.LINK = fid.F_Units
		GROUP BY fid.F_Invoices, fu.N_Code, fu.C_Name
		HAVING COUNT(DISTINCT fu.LINK) = 1
	) units
		ON units.F_Invoices = fi.LINK
	CROSS APPLY dbo.MF_DateMAX14(fi.S_Create_Date, fi.S_Modif_Date, fsc.S_Create_Date, fsc.S_Modif_Date, fdt.S_Create_Date, fdt.S_Modif_Date, fd.S_Create_Date, fd.S_Modif_Date, fs.S_Create_Date, fs.S_Modif_Date, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_invoices s
		ON s.link = fi.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного СчФ в буфер - позже даты последнего изменения чего-то из данных СчФ
	WHERE s.id IS NULL						-- если в буфере нет данного СчФ (причем туда он попал бы уже после всех изменений), то такой СчФ нам подойдет
		AND ss.B_EE = 1
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today)

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано СчФ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке СчФ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка СчФ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Invoices'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка строк СчФ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка строк СчФ', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- строки СчФ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	INSERT INTO [IE].LKK_invoices_details
		(link, d_date_begin, d_date_end, n_invoice, n_suboperation_type, c_suboperation_type,
		n_time_zone, c_time_zone, n_invoce_grp, n_bill_grp, n_tariff, c_tariff,
		n_tariff_amount, n_tax, n_amount, n_quantity, n_tariff_amount0, n_tariff_amount1,
		n_tax0, n_tax1, n_amount0, n_amount1, n_quantity0, n_quantity1,
		n_period, n_unit, c_unit)
	SELECT
		fid.LINK			AS link,
		fid.D_Date_Begin	AS d_date_begin,
		fid.D_Date_End		AS d_date_end,
		fid.F_Invoices		AS n_invoice,
		fsot.N_Code			AS n_suboperation_type,
		fsot.C_Name			AS c_suboperation_type,
		ftz.N_Code			AS n_time_zone,
		ftz.C_Name			AS c_time_zone,
		fid.F_Inv_Grp		AS n_invoce_grp,
		fid.F_Bill_Grp		AS n_bill_grp,
		ISNULL(TRY_CONVERT(SMALLINT, ft.N_Code), 0)
							AS n_tariff,
		ft.C_Name			AS c_tariff,
		fid.N_Tariff		AS n_tariff_amount,
		fid.N_Tax			AS n_tax,
		fid.N_Amount		AS n_amount,
		fid.N_Quantity		AS n_quantity,
		fid.N_Tariff0		AS n_tariff_amount0,
		fid.N_Tariff1		AS n_tariff_amount1,
		fid.N_Tax_Amount0	AS n_tax0,
		fid.N_Tax_Amount1	AS n_tax1,
		fid.N_Amount0		AS n_amount0,
		fid.N_Amount1		AS n_amount1,
		fid.N_Quantity0		AS n_quantity0,
		fid.N_Quantity1		AS n_quantity1,
		fid.N_Period		AS n_period,
		fu.N_Code			AS n_unit,
		fu.C_Name			AS c_unit
	FROM [IE].LKK_invoices fi
	INNER JOIN EE.FVT_Invoices_Details AS fid
		ON fid.F_Invoices = fi.link
	LEFT JOIN dbo.FS_Sub_Operation_Types AS fsot
		ON fsot.LINK = fid.F_Sub_Operation_Types
	LEFT JOIN dbo.FS_Time_Zones AS ftz
		ON ftz.LINK = fid.F_Time_Zones
	LEFT JOIN dbo.FS_Tariff AS ft
		ON ft.LINK = fid.F_Tariff
	LEFT JOIN dbo.FS_Units AS fu
		ON fu.LINK = fid.F_Units
	CROSS APPLY dbo.MF_DateMAX14(fid.S_Create_Date, fid.S_Modif_Date, fsot.S_Create_Date, fsot.S_Modif_Date, ftz.S_Create_Date, ftz.S_Modif_Date, ft.S_Create_Date, ft.S_Modif_Date, fu.S_Create_Date, fu.S_Modif_Date, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_invoices_details s
		ON s.link = fid.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данной Строки СчФ в буфер - позже даты последнего изменения чего-то из данных строки
	WHERE s.id IS NULL						-- если в буфере нет данной Строки СчФ (причем туда он попал бы уже после всех изменений), то такая Строка СчФ нам подойдет

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано строк СчФ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке строк СчФ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка строк СчФ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Invoices_Details'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка счетов]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка счетов', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- Счета
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	INSERT INTO [IE].LKK_bills
		(link, n_subscr, n_sale_category, c_sale_category, n_doctype, c_doctype,
		n_invoice, n_debts, c_debts, n_operationtype, c_operationtype,
		n_status, c_status, d_date, d_date_begin, d_date_end, d_date_due,
		c_number, n_period, n_amount, n_tax, n_quantity, n_cons, n_amount_topay,
		b_closed)
	SELECT
		fb.LINK						AS link,
		ss.LINK						AS n_subscr,
		fsc.N_Code					AS n_sale_category,
		fsc.C_Name					AS c_sale_category,
		fdt.N_Code					AS n_doctype,
		fdt.C_Name					AS c_doctype,
		fb.F_Invoices				AS n_invoice,
		fd.N_Code					AS n_debts,
		fd.C_Name					AS c_debts,
		fot.N_Code					AS n_operationtype,
		fot.C_Name					AS c_operationtype,
		fs.LINK						AS n_status,
		fs.C_Name					AS c_status,
		fb.D_Date					AS d_date,
		fb.D_Date_Begin				AS d_date_begin,
		fb.D_Date_End				AS d_date_end,
		fb.D_Date_Due				AS d_date_due,
		fb.C_Number					AS c_number,
		fb.N_Period					AS n_period,
		fb.N_Amount					AS n_amount,
		fb.N_Tax_Amount				AS n_tax,
		fb.N_Quantity				AS n_quantity,
		fb.N_Cons					AS n_cons,
		fb.N_Amount_ToPay			AS n_amount_topay,
		fb.B_Closed					AS b_closed
	FROM dbo.SD_Subscr AS ss
	INNER JOIN EE.FD_Bills AS fb
		ON fb.F_Subscr = ss.LINK
	INNER JOIN dbo.FS_Sale_Categories AS fsc
		ON fsc.LINK = fb.F_Sale_Categories
	INNER JOIN dbo.FS_Doc_Types AS fdt
		ON fdt.LINK = fb.F_Doc_Types
	INNER JOIN dbo.FS_Debts AS fd
		ON fd.LINK = fb.F_Debts
	INNER JOIN dbo.FS_Operation_Types AS fot
		ON fot.LINK = fb.F_Operation_Types
	INNER JOIN dbo.FS_Status AS fs
		ON fs.LINK = fb.F_Status
	CROSS APPLY dbo.MF_DateMAX14(fb.S_Create_Date, fb.S_Modif_Date, fsc.S_Create_Date, fsc.S_Modif_Date, fdt.S_Create_Date, fdt.S_Modif_Date, fd.S_Create_Date, fd.S_Modif_Date, fot.S_Create_Date, fot.S_Modif_Date, fs.S_Create_Date, fs.S_Modif_Date, NULL, NULL) md
	LEFT JOIN [IE].LKK_bills s
		ON s.link = fb.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного Счета в буфер - позже даты последнего изменения чего-то из данных Счета
	WHERE s.id IS NULL						-- если в буфере нет данного Счета (причем туда он попал бы уже после всех изменений), то такой Счет нам подойдет
		AND ss.B_EE = 1
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today)

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано счетов: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке счетов произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка счетов отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Bills'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка строк счетов]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка строк счетов', 
		@step_id=11, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- строки счетов
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	INSERT INTO [IE].LKK_bills_details
		(link, /*f_bill, */n_bill, d_date_begin, d_date_end, n_suboperation_type, c_suboperation_type,
		n_time_zone, c_time_zone, n_invoce_grp, n_bill_grp, n_tariff_amount, c_tariff, n_tariff,
		n_tax, n_amount, n_quantity, n_period)
	SELECT
		fbd.LINK			AS link,
		--fb.id				AS f_bill,
		fb.link				AS n_bill,
		fbd.D_Date_Begin	AS d_date_begin,
		fbd.D_Date_End		AS d_date_end,
		fsot.N_Code			AS n_suboperation_type,
		fsot.C_Name			AS c_suboperation_type,
		ftz.N_Code			AS n_time_zone,
		ftz.C_Name			AS c_time_zone,
		fbd.F_Inv_Grp		AS n_invoce_grp,
		fbd.F_Bill_Grp		AS n_bill_grp,
		fbd.N_Tariff		AS n_tariff_amount,
		ft.C_Name			AS c_tariff,
		ISNULL(TRY_CONVERT(SMALLINT, ft.N_Code), 0)
							AS n_tariff,
		fbd.N_Tax			AS n_tax,
		fbd.N_Amount		AS n_amount,
		fbd.N_Quantity		AS n_quantity,
		fbd.N_Period		AS n_period
	FROM [IE].LKK_bills fb
	INNER JOIN EE.FVT_Bills_Details AS fbd
		ON fbd.F_Bills = fb.link
	LEFT JOIN dbo.FS_Sub_Operation_Types AS fsot
		ON fsot.LINK = fbd.F_Sub_Operation_Types
	LEFT JOIN dbo.FS_Time_Zones AS ftz
		ON ftz.LINK = fbd.F_Time_Zones
	LEFT JOIN dbo.FS_Tariff AS ft
		ON ft.LINK = fbd.F_Tariff
	CROSS APPLY dbo.MF_DateMAX14(fbd.S_Create_Date, fbd.S_Modif_Date, fsot.S_Create_Date, fsot.S_Modif_Date, ftz.S_Create_Date, ftz.S_Modif_Date, ft.S_Create_Date, ft.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_bills_details s
		ON s.link = fbd.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данной Строки счета в буфер - позже даты последнего изменения чего-то из данных строки
	WHERE s.id IS NULL						-- если в буфере нет данной Строки счета (причем туда он попал бы уже после всех изменений), то такая Строка счета нам подойдет

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано строк счетов: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке строк счетов произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка строк счетов отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Bills_Details'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка платежей]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка платежей', 
		@step_id=12, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- платежи
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	INSERT INTO [IE].LKK_payments
		(link, n_subscr, d_date, n_period, c_number, n_amount, c_destination)
	SELECT
		fp.LINK						AS link,
		ss.LINK						AS n_subscr,
		fp.D_Date					AS d_date,
		fp.N_Period					AS n_period,
		fp.C_Number					AS c_number,
		fp.N_Amount					AS n_amount,
		fp.C_Destination			AS c_destination
	FROM dbo.SD_Subscr AS ss
	INNER JOIN EE.FD_Payments AS fp
		ON fp.F_Subscr = ss.LINK
	CROSS APPLY dbo.MF_DateMAX14(fp.S_Create_Date, fp.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].LKK_payments s
		ON s.link = fp.LINK
		AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного Счета в буфер - позже даты последнего изменения чего-то из данных Счета
	WHERE s.id IS NULL						-- если в буфере нет данного Счета (причем туда он попал бы уже после всех изменений), то такой Счет нам подойдет
		AND ss.B_EE = 1
		-- ЛС не закрыт
		AND ss.D_Date_Begin < @today
		AND (ss.D_Date_End IS NULL OR ss.D_Date_End > @today)

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано платежей: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке платежей произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка платежей отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Payments'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Формирование PDF СчФ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Формирование PDF СчФ', 
		@step_id=13, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- формирование PDF с СчФ
SET @success = 0;
SET @msg = NULL;
SET @count = 0;
SET @step_start = GETDATE();
SET @step_end = NULL;

BEGIN
	--	Шаблон таблицы с CMD
	--	Можно передать либо готовую CMD в поле C_CMD, либо только основные параметры в C_File_Name, C_Report и C_Parameter, по которым CMD сформируется автоматом
	IF OBJECT_ID (''tempdb.dbo.#CMDShell'') IS NOT NULL DROP TABLE #CMDShell
	CREATE TABLE #CMDShell
	(
		LINK			INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,

		C_File_Name		VARCHAR(8000),		--	наименование файла для формируемого в CMD отчета, без расширения
		C_Report		VARCHAR(8000),		--	Наименование RDL, без расширения файла
											--	Пример: "[T19] Акт сверки" (без кавычек)
		C_Parameter		VARCHAR(8000),		--	Параметры отчета
											--	Пример:	  '' D_Date=''		+ CONVERT(VARCHAR(20), @D_Date, 104)
											--			+ '' F_Division=''	+ CAST (@F_Division AS VARCHAR(3))
											--			+ '' B_Create_Doc=''	+ ''false''
		C_CMD			VARCHAR(8000),		--	Готовая исполяемая команда
		C_Error_Prefix	VARCHAR(1000),		--	Префикс к генерируемому сообщению об ошибке.
			              	              	--	Какая то конкретика: наименование отчета, параметры однозначно идентифицирующие место ошибки (например номер ЛС, объект, ТоП и пр.)
											--	Пример: "Ошибка при выполнении операции формирования отчета "Акт сверки". Не удалось построить отчет по ЛС N_Code."
		B_Error			BIT DEFAULT(0),		--	Признак ошибки при выполнении
		C_Error			VARCHAR(max)		--	Текст ошибки
	);

	--	Список ЛС и СчФ
	IF OBJECT_ID(''tempdb..#T_Reports'') IS NOT NULL DROP TABLE #T_Reports;
	CREATE TABLE #T_Reports
	(
		LINK				INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		F_Subscr			VARCHAR(10),
		N_Subscr			VARCHAR(25),
		F_Invoices			VARCHAR(19),
		F_Bills				VARCHAR(19)
	);

	--	Список отчетов
	INSERT INTO #T_Reports (F_Subscr, N_Subscr, F_Invoices)
		SELECT
			CAST(ss.LINK AS VARCHAR(10))		AS F_Subscr,
			CAST(ss.N_Code AS VARCHAR(10))		AS N_Subscr,
			CAST(fb.F_Invoices AS VARCHAR(19))	AS F_Invoices
		FROM dbo.SD_Subscr AS ss
		--	Счет в заданном РП
		INNER JOIN EE.FD_Bills AS fb
			ON	fb.F_Subscr			= ss.LINK
		INNER JOIN dbo.FS_Status AS fs
			ON	fs.LINK				= fb.F_Status
			AND fs.B_Reversed		= 0
			AND fs.B_Reversing		= 0
		CROSS APPLY dbo.MF_DateMAX14(ss.S_Create_Date, ss.S_Modif_Date, fb.S_Create_Date, fb.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) md
		LEFT JOIN [IE].LKK_reports s
			ON s.link = fb.F_Invoices
			AND s.n_doc_type = 11
			AND s.d_import > md.D_Date0					-- дата предыдущего импорта данного файла в буфер - позже даты последнего изменения чего-то из данных Счета
		WHERE	ss.B_EE				= 1					-- Cчета ЮЛ
			AND ss.LINK				<>-ss.F_Division	-- Без нулевого абона
			AND fb.F_Invoices IS NOT NULL
			AND	s.id IS NULL							-- нет контента для СчФ с таким линком
		GROUP BY ss.LINK, ss.N_Code, fb.F_Invoices
		--	Упорядочим, чтобы далее обработать случай нескольких документов в РП
		ORDER BY ss.LINK, fb.F_Invoices DESC;

	INSERT INTO #CMDShell (
		C_File_Name, C_Report, C_Parameter, C_Error_Prefix )
	SELECT
		''СчФ_'' + s.N_Subscr + ''_01_'' + s.F_Invoices
									AS C_File_Name,
		''СчФ (Постановление 1137)''	AS C_Report,
			'' LINK='' + s.F_Invoices	AS C_Parameter,
		''Ошибка при выполнении операции "Печать расчетных документов"!'' +
		''Ошибка при формирования отчета "СчФ (Постановление 1137)". Не удалось построить отчет по ЛС '' + s.N_Subscr + '', Ид-р Счет-Фактуры: '' + s.F_Invoices
									AS C_Error_Prefix
	FROM #T_Reports AS s;

	SET @count = @@ROWCOUNT;
	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Подготовлено СчФ к формированию PDF: '' + CONVERT(VARCHAR(10), @count);

	DECLARE @Status_Msg VARCHAR(1000);
	-- каталог для файлов
	DECLARE @c_Target_Dir NVARCHAR(300) = dbo.CF_UIVars_Values_Get_Str (''C_AdvicePDFResult_Path'', NULL);

	-- в нём будем искать файлы с именем типа YYYYMMDDHHMi*_{SUSER_NAME()} + оригинальный C_File_Name + ".pdf"
	DECLARE @filename_prefix_mask VARCHAR(100);
	SET @filename_prefix_mask = CONVERT(VARCHAR(8), GETDATE(), 112) + ''*_'' + REPLACE(CAST(SUSER_NAME() AS VARCHAR(100)), ''\'', ''_'') + ''*'';	-- маска вида 20180903*_domainname_username* в расчете, что вычисление маски здесь и в ХП svc.CP_Cmdshell_Multiple не попадёт в разные сутки :)

	-- формируем все PDF (папка отчетов - в конфиге репортбилдера)
	EXEC SVC.CP_CMDShell_Multiple
		@F_Division		= 0,
		@F_SubDivision	= 0,
		@F_Type_File	= 2,				-- PDF
		@C_Merge_Name	= ''_Печать_СчФ'',
		@ExtParam		= NULL,
		@F_Merge		= 0,				-- не склеивать
		@Status_Msg		= @Status_Msg OUTPUT;

	SET @step_end = GETDATE();
	SET @msg = @msg + '' (формирование PDF: '' + CONVERT(varchar(30), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';
	IF @Status_Msg <> ''''
		SET @msg = @msg + CHAR(13) + CHAR(10) + @Status_Msg;

	DECLARE @files TABLE(line VARCHAR(1000), link INT IDENTITY(1,1));
	DECLARE @cmd_dir VARCHAR(255);
	SET @cmd_dir = ''DIR "'' + @C_Target_Dir + ''\'' + @filename_prefix_mask + ''"'';

	INSERT INTO @files EXEC svc.CP_cmdshell @cmd_dir;

	-- если каталог не пустой, и в нём есть файлы по маске, переберём их
	IF NOT EXISTS (SELECT * FROM @files WHERE line IN (''The network path was not found.'', ''Не найден сетевой путь.'', ''The system cannot find the path specified.'', ''Системе не удается найти указанный путь.'', ''File Not Found'', ''Файл не найден''))
	BEGIN
		-- перебираем каждый файл
		DECLARE @file_link INT;
		SELECT @file_link = MAX(link) FROM #CMDShell AS c;
		DECLARE @filename VARCHAR(100);
		DECLARE @line VARCHAR(1000);
		DECLARE @F_Invoice BIGINT;
		DECLARE @content_table AS TABLE (file_content VARBINARY(MAX), F_Invoice BIGINT);

		DELETE FROM @content_table;

		WHILE @file_link > 0
		BEGIN
			SET @filename = NULL;
			SET @line = NULL;
			SET @F_invoice = NULL;
			SELECT @filename = C_File_Name + ''.pdf'' FROM #CMDShell WHERE LINK = @file_link;
			SELECT @line = line FROM @files WHERE line LIKE ''%'' + @filename + ''%'';

			-- если файла нет, то пока ладно
			-- если есть - вытащим наконец правильный путь
			IF @line IS NOT NULL
			BEGIN
				SELECT @F_Invoice = /*''_01_'' + s.F_Invoices*/ CONVERT(BIGINT, SUBSTRING(@filename, CHARINDEX(''_01_'', @filename) + 4, LEN(@filename) - CHARINDEX(''_01_'', @filename) - 7));
				SET @filename = @C_Target_Dir  + ''\'' + RIGHT(@line, LEN(@line) - 36);	-- 36 - начало строки в выводе DIR (дата создания и размер файла)

				INSERT INTO @content_table(file_content)
					EXEC dbo.IMP_File_Copy
						@FileName = @filename,
						@File = NULL;

				UPDATE @content_table SET F_Invoice = @F_Invoice WHERE F_Invoice IS NULL;
			END;

			IF @F_Invoice IS NOT NULL AND NOT EXISTS(SELECT * FROM @content_table WHERE F_Invoice = @F_Invoice AND file_content IS NOT NULL)
			BEGIN
				SELECT @msg = @msg + ISNULL(''. '' + C_Error, '''') + CHAR(13) + CHAR(10) FROM #CMDShell WHERE LINK = @file_link AND B_Error = 1;
			END;

			SET @file_link = @file_link - 1;
		END

		BEGIN TRY
			INSERT INTO [IE].LKK_reports(link, c_content, n_doc_type )
				SELECT F_Invoice, file_content, 11
				FROM @content_table
				WHERE F_Invoice IS NOT NULL
					AND file_content IS NOT NULL;

			PRINT(@F_Invoice);
		END TRY
		BEGIN CATCH
			SET @msg = @msg + CHAR(13) + CHAR(10) + ''При записи файлов СчФ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''СчФ пропущено: '' + CONVERT(VARCHAR(10), ISNULL((SELECT COUNT(*) FROM @content_table WHERE F_Invoice IS NOT NULL), 0));
			PRINT ERROR_MESSAGE();
		END CATCH;

		-- очищаем выходной каталог по префиксу

		SET @cmd_dir = ''del "'' + @C_Target_Dir + ''\'' + @filename_prefix_mask + ''.pdf"'';
		EXEC svc.CP_cmdshell @cmd_dir;
	END;

	--IF OBJECT_ID(''tempdb..#T_Reports'') IS NOT NULL DROP TABLE #T_Reports;
	--IF OBJECT_ID (''tempdb.dbo.#CMDShell'') IS NOT NULL DROP TABLE #CMDShell;

END;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Invoices'', 10 /*COT_Insert_Mass*/, @msg);

IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;

GO
', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=4
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Выгрузка профилей ПУ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Выгрузка профилей ПУ', 
		@step_id=14, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- ПУ
SET @success = 0;
SET @msg = NULL;
SET @step_start = GETDATE();
SET @step_end = NULL;
BEGIN TRY

	IF OBJECT_ID(''tempdb..#T_subscr_devices'') IS NOT NULL DROP TABLE #T_subscr_devices
	CREATE TABLE #T_subscr_devices (link bigint, id uniqueidentifier)
	CREATE CLUSTERED INDEX [IDC_T_Subscr_Devices] ON #T_subscr_devices (link ASC)

	INSERT INTO #T_subscr_devices (link, id)	
	SELECT link, id	
	FROM [IE].[LKK_subscr_devices]
	GROUP BY link, id

	IF OBJECT_ID(''tempdb..#T_meters_profiles'') IS NOT NULL DROP TABLE #T_meters_profiles
	CREATE TABLE #T_meters_profiles (link bigint, id uniqueidentifier, d_import datetime)
	CREATE CLUSTERED INDEX [IDC_T_meters_profiles] ON #T_meters_profiles (link ASC)

	INSERT INTO #T_meters_profiles (link, d_import)
	SELECT link, d_import
	FROM [IE].[LKK_meters_profiles] s
	GROUP BY link, d_import

	IF OBJECT_ID(''tempdb..#T_meters_profiles_imp'') IS NOT NULL DROP TABLE #T_meters_profiles_imp
	CREATE TABLE #T_meters_profiles_imp (link bigint, f_device uniqueidentifier, n_device bigint, D_Date datetime, N_Hour int, n_period int, c_timezone varchar(16), c_energytype varchar(4), n_cons decimal(19,6), d_import datetime, N_Row bigint)

	-- данные по ПУ, еще отсутствующим в буфере
	INSERT INTO #T_meters_profiles_imp		
		(link, f_device, n_device, D_Date, N_Hour, n_period,
		c_timezone, c_energytype, n_cons, N_Row)

	SELECT			
		emp.LINK * 10000 + cuv.id							AS LINK, 
		CONVERT(uniqueidentifier,dev.id)					AS f_device, 
		CONVERT(bigint,ed.LINK)								AS n_device, 		
		CONVERT(datetime,CONVERT(varchar(8),emp.n_period * 100 + (cuv.Id / 24 + 1)))	AS D_Date,
		cuv.Id % 24 + 1										AS N_Hour, 
		emp.N_Period										AS n_period,
		''Сутки''												AS c_timezone,
		eet.C_Short_Name									AS c_energytype,
		CONVERT(decimal(19,6),cuv.Value1)					AS n_cons,
		ROW_Number () OVER (ORDER BY dev.id)				AS N_Row

	FROM dbo.ED_Meter_Profiles emp
		INNER JOIN dbo.ED_Devices ed
			ON ed.LINK = emp.F_Devices
		INNER JOIN #T_subscr_devices dev																																--	идентификатор ПУ из ЛКК
			ON dev.link = ed.LINK
		CROSS APPLY dbo.EF_UnpackValues(emp.N_Cons, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, emp.N_Param15) AS cuv		--	распакуем профиль
		LEFT JOIN dbo.ES_Readings_Status AS ers
			ON	ers.LINK		= cuv.Value17	
			AND ers.C_Const <> ''ERS_NoValid''																															-- все статусы, кроме недост
		INNER JOIN dbo.ES_Energy_Types eet			
			ON eet.LINK = emp.F_Energy_Types
		CROSS APPLY dbo.MF_DateMAX14(ed.S_Create_Date, ed.S_Modif_Date, emp.S_Create_Date, emp.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) md
		LEFT JOIN #T_meters_profiles s
			ON s.link = (emp.LINK * 10000 + cuv.id)
			AND s.d_import > md.D_Date0			-- дата предыдущего импорта данного ПУ в буфер - позже даты последнего изменения чего-то из данных ПУ
		WHERE s.link IS NULL

	-- данные по ПУ, еще отсутствующим в буфере
	DECLARE @i int = 1
	WHILE (SELECT COUNT(*) FROM #T_meters_profiles_imp) > 0
		BEGIN
			SET @i = @i + 2000
			INSERT INTO [IE].[LKK_meters_profiles]
				(link, f_device, n_device, D_Date, N_Hour, n_period,
				c_timezone, c_energytype, n_cons)
			SELECt link, f_device, n_device, D_Date, N_Hour, n_period,
				c_timezone, c_energytype, n_cons 
			FROM #T_meters_profiles_imp
			WHERE N_Row < @i

			DELETE FROM #T_meters_profiles_imp WHERE N_Row < @i
			PRINT (@i)

		END
		;
		;

	IF OBJECT_ID(''tempdb..#T_subscr_devices'') IS NOT NULL DROP TABLE #T_subscr_devices
	IF OBJECT_ID(''tempdb..#T_meters_profiles'') IS NOT NULL DROP TABLE #T_meters_profiles
	IF OBJECT_ID(''tempdb..#T_meters_profiles_imp'') IS NOT NULL DROP TABLE #T_meters_profiles_imp

	SET @count = @@ROWCOUNT;
	SET @step_end = GETDATE();

	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Передано профилей ПУ: '' + CONVERT(VARCHAR(10), @count) + '' ('' + CONVERT(varchar(10), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';

	SET @success = 1;
END TRY
BEGIN CATCH
	SET @msg = ''При выгрузке профилей ПУ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''Выгрузка профилей ПУ отменена'';
	SET @success = 0;
END CATCH;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''dbo.ED_Meter_Profiles'', 4 /*COT_Execute*/, @msg);
IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;
GO', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Формирование PDF РВ]    Script Date: 10.09.2018 17:21:47 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Формирование PDF РВ', 
		@step_id=15, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=3, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'SET ANSI_NULLS ON;
GO
SET XACT_ABORT ON;
GO
SET NOCOUNT ON;
GO
SET ANSI_WARNINGS ON;
GO

-- на всю выгрузку
DECLARE @today SMALLDATETIME;
DECLARE @d_today DATE;
DECLARE @year SMALLINT;
DECLARE @month TINYINT;
DECLARE @period INT;
SET @today = GETDATE();
SET @d_today = CONVERT(DATE, @today);
SET @year = YEAR(@today);
SET @month = MONTH(@today);
SET @period = @year * 100 + @month;

-- на каждый этап
DECLARE @msg VARCHAR(MAX);
DECLARE @success BIT = 0;
DECLARE @count INT = 0;
DECLARE @step_start DATETIME;
DECLARE @step_end DATETIME;

-- формирование PDF с СчФ
SET @success = 0;
SET @msg = NULL;
SET @count = 0;
SET @step_start = GETDATE();
SET @step_end = NULL;

BEGIN
	--	Шаблон таблицы с CMD
	--	Можно передать либо готовую CMD в поле C_CMD, либо только основные параметры в C_File_Name, C_Report и C_Parameter, по которым CMD сформируется автоматом
	IF OBJECT_ID (''tempdb.dbo.#CMDShell'') IS NOT NULL DROP TABLE #CMDShell
	CREATE TABLE #CMDShell
	(
		LINK			INT IDENTITY(1,1) PRIMARY KEY NONCLUSTERED,

		C_File_Name		VARCHAR(8000),		--	наименование файла для формируемого в CMD отчета, без расширения
		C_Report		VARCHAR(8000),		--	Наименование RDL, без расширения файла
											--	Пример: "[T19] Акт сверки" (без кавычек)
		C_Parameter		VARCHAR(8000),		--	Параметры отчета
											--	Пример:	  '' D_Date=''		+ CONVERT(VARCHAR(20), @D_Date, 104)
											--			+ '' F_Division=''	+ CAST (@F_Division AS VARCHAR(3))
											--			+ '' B_Create_Doc=''	+ ''false''
		C_CMD			VARCHAR(8000),		--	Готовая исполяемая команда
		C_Error_Prefix	VARCHAR(1000),		--	Префикс к генерируемому сообщению об ошибке.
			              	              	--	Какая то конкретика: наименование отчета, параметры однозначно идентифицирующие место ошибки (например номер ЛС, объект, ТоП и пр.)
											--	Пример: "Ошибка при выполнении операции формирования отчета "Акт сверки". Не удалось построить отчет по ЛС N_Code."
		B_Error			BIT DEFAULT(0),		--	Признак ошибки при выполнении
		C_Error			VARCHAR(max)		--	Текст ошибки
	);

	--	Список ЛС и СчФ
	IF OBJECT_ID(''tempdb..#T_Reports'') IS NOT NULL DROP TABLE #T_Reports;
	CREATE TABLE #T_Reports
	(
		LINK				INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
		F_Subscr			VARCHAR(10),
		N_Subscr			VARCHAR(25),
		F_Invoices			VARCHAR(19),
		F_Bills				VARCHAR(19),
		F_Paysheets			VARCHAR(19),
		F_Division			VARCHAR(19)
	);

	--	Список отчетов
	INSERT INTO #T_Reports (F_Division, F_Subscr, N_Subscr, F_Paysheets)
	-- РВ
	SELECT
		CAST(ss.F_Division AS VARCHAR(10))	AS F_Division,
		CAST(ss.LINK AS VARCHAR(10))		AS F_Subscr,
		CAST(ss.N_Code AS VARCHAR(10))		AS N_Subscr,
		CAST(FP.LINK AS VARCHAR(19))		AS F_Paysheets
	FROM dbo.SD_Subscr AS ss
	INNER JOIN EE.FD_Paysheets FP										--	select top 10 * from EE.FD_Paysheets FP
		ON  ss.link = FP.F_Subscr
	INNER JOIN dbo.FS_Status AS fs
		ON	fs.LINK				= FP.F_Status
		AND fs.B_Reversed		= 0
		AND fs.B_Reversing		= 0
	CROSS APPLY dbo.MF_DateMAX14(ss.S_Create_Date, ss.S_Modif_Date, FP.S_Create_Date, FP.S_Modif_Date, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) md
	LEFT JOIN [IE].[LKK_reports_view] s										
		ON s.link = FP.LINK
		AND s.n_doc_type = 120
		AND s.d_import > md.D_Date0	
	WHERE ss.B_EE				= 1					-- Cчета ЮЛ
		AND ss.LINK				<>-ss.F_Division	-- Без нулевого абона
		AND FP.LINK IS NOT NULL
		AND	s.link IS NULL
	GROUP BY ss.F_Division, ss.LINK, ss.N_Code, FP.LINK
	ORDER BY ss.F_Division, ss.LINK, FP.LINK DESC;

	INSERT INTO #CMDShell (
		C_File_Name, C_Report, C_Parameter, C_Error_Prefix )
	SELECT
		''РВ_'' + s.N_Subscr + ''_01_'' + s.F_Paysheets
									AS C_File_Name,
		''Приложение №8 Акт приема-передачи электроэнергии по документу''	AS C_Report,
			'' LINK='' + s.F_Paysheets AS C_Parameter,
		''Ошибка при выполнении операции "Печать расчетных документов"!'' +
		''Ошибка при формирования отчета " Акт приема-передачи электроэнергии по документу". Не удалось построить отчет по ЛС '' + s.N_Subscr + '', Ид-р Расчетной ведомости: '' + s.F_Paysheets
									AS C_Error_Prefix
	FROM #T_Reports AS s;			--	select * from #CMDShell	--	 LINK=42380 F_Division=1

	SET @count = @@ROWCOUNT;
	SET @msg = ISNULL(@msg + CHAR(13) + CHAR(10), '''') + ''Подготовлено РВ к формированию PDF: '' + CONVERT(VARCHAR(10), @count);

	DECLARE @Status_Msg VARCHAR(1000);
	-- каталог для файлов
	DECLARE @c_Target_Dir NVARCHAR(300) = dbo.CF_UIVars_Values_Get_Str (''C_AdvicePDFResult_Path'', NULL);

	-- в нём будем искать файлы с именем типа YYYYMMDDHHMi*_{SUSER_NAME()} + оригинальный C_File_Name + ".pdf"
	DECLARE @filename_prefix_mask VARCHAR(100);
	SET @filename_prefix_mask = CONVERT(VARCHAR(8), GETDATE(), 112) + ''*_'' + REPLACE(CAST(SUSER_NAME() AS VARCHAR(100)), ''\'', ''_'') + ''*'';	-- маска вида 20180903*_domainname_username* в расчете, что вычисление маски здесь и в ХП svc.CP_Cmdshell_Multiple не попадёт в разные сутки :)

	-- формируем все PDF (папка отчетов - в конфиге репортбилдера)
	EXEC SVC.CP_CMDShell_Multiple
		@F_Division		= 0,
		@F_SubDivision	= 0,
		@F_Type_File	= 2,				-- PDF
		@C_Merge_Name	= ''_Печать_РВ'',
		@ExtParam		= NULL,
		@F_Merge		= 0,				-- не склеивать
		@Status_Msg		= @Status_Msg OUTPUT;

	SET @step_end = GETDATE();
	SET @msg = @msg + '' (формирование PDF: '' + CONVERT(varchar(30), DATEDIFF(ms, @step_start, @step_end) / 1000.0) + '' сек)'';
	IF @Status_Msg <> ''''
		SET @msg = @msg + CHAR(13) + CHAR(10) + @Status_Msg;

	DECLARE @files TABLE(line VARCHAR(1000), link INT IDENTITY(1,1));
	DECLARE @cmd_dir VARCHAR(255);
	SET @cmd_dir = ''DIR "'' + @C_Target_Dir + ''\'' + @filename_prefix_mask + ''"'';

	INSERT INTO @files EXEC svc.CP_cmdshell @cmd_dir;

	-- если каталог не пустой, и в нём есть файлы по маске, переберём их
	IF NOT EXISTS (SELECT * FROM @files WHERE line IN (''The network path was not found.'', ''Не найден сетевой путь.'', ''The system cannot find the path specified.'', ''Системе не удается найти указанный путь.'', ''File Not Found'', ''Файл не найден''))
	BEGIN
		-- перебираем каждый файл
		DECLARE @file_link INT;
		SELECT @file_link = MAX(link) FROM #CMDShell AS c;
		DECLARE @filename VARCHAR(100);
		DECLARE @line VARCHAR(1000);
		DECLARE @F_Paysheets BIGINT;
		DECLARE @content_table AS TABLE (file_content VARBINARY(MAX), F_Paysheets BIGINT);

		DELETE FROM @content_table;

		WHILE @file_link > 0
		BEGIN
			SET @filename = NULL;
			SET @line = NULL;
			SET @F_Paysheets = NULL;
			SELECT @filename = C_File_Name + ''.pdf'' FROM #CMDShell WHERE LINK = @file_link;
			SELECT @line = line FROM @files WHERE line LIKE ''%'' + @filename + ''%'';

			-- если файла нет, то пока ладно
			-- если есть - вытащим наконец правильный путь
			IF @line IS NOT NULL
			BEGIN
				SELECT @F_Paysheets = /*''_01_'' + s.F_Invoices*/ CONVERT(BIGINT, SUBSTRING(@filename, CHARINDEX(''_01_'', @filename) + 4, LEN(@filename) - CHARINDEX(''_01_'', @filename) - 7));
				SET @filename = @C_Target_Dir  + ''\'' + RIGHT(@line, LEN(@line) - 36);	-- 36 - начало строки в выводе DIR (дата создания и размер файла)

				INSERT INTO @content_table(file_content)
					EXEC dbo.IMP_File_Copy
						@FileName = @filename,
						@File = NULL;

				UPDATE @content_table SET F_Paysheets = @F_Paysheets WHERE F_Paysheets IS NULL;
			END;

			IF @F_Paysheets IS NOT NULL AND NOT EXISTS(SELECT * FROM @content_table WHERE F_Paysheets = @F_Paysheets AND file_content IS NOT NULL)
			BEGIN
				SELECT @msg = @msg + ISNULL(''. '' + C_Error, '''') + CHAR(13) + CHAR(10) FROM #CMDShell WHERE LINK = @file_link AND B_Error = 1;
			END;

			SET @file_link = @file_link - 1;
		END

		BEGIN TRY
			INSERT INTO [IE].LKK_reports(link, c_content, n_doc_type)
				SELECT F_Paysheets, file_content, 120
				FROM @content_table
				WHERE F_Paysheets IS NOT NULL
					AND file_content IS NOT NULL;

			PRINT(@F_Paysheets);
		END TRY
		BEGIN CATCH
			SET @msg = @msg + CHAR(13) + CHAR(10) + ''При записи файлов РВ произошла ошибка '' + ERROR_MESSAGE() + CHAR(13) + CHAR(10) + ''РВ пропущено: '' + CONVERT(VARCHAR(10), ISNULL((SELECT COUNT(*) FROM @content_table WHERE F_Paysheets IS NOT NULL), 0));
			PRINT ERROR_MESSAGE();
		END CATCH;

		-- очищаем выходной каталог по префиксу

		SET @cmd_dir = ''del "'' + @C_Target_Dir + ''\'' + @filename_prefix_mask + ''.pdf"'';
		EXEC svc.CP_cmdshell @cmd_dir;
	END;

	--IF OBJECT_ID(''tempdb..#T_Reports'') IS NOT NULL DROP TABLE #T_Reports;
	--IF OBJECT_ID (''tempdb.dbo.#CMDShell'') IS NOT NULL DROP TABLE #CMDShell;

END;

PRINT(@msg);
INSERT INTO dbo.CD_System_Log
(
	F_Division,
	C_TableName,
	F_Oper_Types,
	C_Notes
)
	VALUES (0, ''EE.FD_Paysheets'', 10 /*COT_Insert_Mass*/, @msg);

IF (@success = 0)
BEGIN
	INSERT INTO dbo.CS_Error_Log
	(
		F_Division,
		C_Error_Text
	)
	VALUES (0, @msg);
END;

GO', 
		@database_name=N'_OmniUS_TMP_309_RG_3', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'CollectorSchedule_Every_6h', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=8, 
		@freq_subday_interval=6, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20120210, 
		@active_end_date=99991231, 
		@active_start_time=0, 
		@active_end_time=235959, 
		@schedule_uid=N'e2462219-8e8e-40e8-85ad-5d897a2e24f9'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO
