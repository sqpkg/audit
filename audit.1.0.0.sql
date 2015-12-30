EXECUTE sqpkg.sp_create_package 'audit'
	,'Audit'
	,'1.0.0'
GO

CREATE SCHEMA [#audit]
GO

CREATE PROCEDURE [#audit].[sp_alter_audit_on] (
	@source_table SYSNAME
	,@source_schema SYSNAME = 'dbo'
	,@destination_table SYSNAME = NULL
	,@destination_schema SYSNAME = 'audit'
	,@expiration_in_days INT = 90
	)
AS
SET NOCOUNT ON
SET @destination_table = ISNULL(@destination_table, @source_table)

DECLARE @source_qualified SYSNAME = '[' + @source_schema + '].[' + @source_table + ']'
DECLARE @source_trigger SYSNAME = '[' + @source_schema + '].[' + @source_table + '_audit]'
DECLARE @destination_qualified SYSNAME = '[' + @destination_schema + '].[' + @destination_table + ']'
DECLARE @sql NVARCHAR(MAX)
DECLARE @columns NVARCHAR(max) = ''

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE object_id = OBJECT_ID(@source_qualified)
		)
BEGIN
	IF EXISTS (
			SELECT 1
			FROM sys.tables
			WHERE object_id = OBJECT_ID(@destination_qualified)
			)
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION

			PRINT 'Updating or creating audit.destination property on ' + @source_qualified

			IF NOT EXISTS (
					SELECT 1
					FROM sys.extended_properties
					WHERE major_id = OBJECT_ID(@source_qualified)
						AND [name] = 'audit.destination'
					)
			BEGIN
				EXECUTE sys.sp_addextendedproperty 'audit.destination'
					,@destination_qualified
					,'SCHEMA'
					,@source_schema
					,'TABLE'
					,@source_table
			END
			ELSE
			BEGIN
				EXECUTE sys.sp_updateextendedproperty 'audit.destination'
					,@destination_qualified
					,'SCHEMA'
					,@source_schema
					,'TABLE'
					,@source_table
			END

			PRINT 'Updating or creating audit.expire_after property on ' + @source_qualified

			IF NOT EXISTS (
					SELECT 1
					FROM sys.extended_properties
					WHERE major_id = OBJECT_ID(@source_qualified)
						AND [name] = 'audit.expire_after'
					)
			BEGIN
				EXECUTE sys.sp_addextendedproperty 'audit.expire_after'
					,@expiration_in_days
					,'SCHEMA'
					,@source_schema
					,'TABLE'
					,@source_table
			END
			ELSE
			BEGIN
				EXECUTE sys.sp_updateextendedproperty 'audit.expire_after'
					,@expiration_in_days
					,'SCHEMA'
					,@source_schema
					,'TABLE'
					,@source_table
			END

			PRINT 'Updating or creating audit.source property on ' + @destination_qualified

			IF NOT EXISTS (
					SELECT 1
					FROM sys.extended_properties
					WHERE major_id = OBJECT_ID(@destination_qualified)
						AND [name] = 'audit.source'
					)
			BEGIN
				EXECUTE sys.sp_addextendedproperty 'audit.source'
					,@source_qualified
					,'SCHEMA'
					,@destination_schema
					,'TABLE'
					,@destination_table
			END
			ELSE
			BEGIN
				EXECUTE sys.sp_updateextendedproperty 'audit.source'
					,@source_qualified
					,'SCHEMA'
					,@destination_schema
					,'TABLE'
					,@destination_table
			END

			PRINT 'Updating or creating trigger ' + @source_trigger + ' on ' + @source_table

			SET @columns = Stuff((
						SELECT ',' + '[' + [name] + ']'
						FROM sys.columns
						WHERE OBJECT_ID = OBJECT_ID(@destination_qualified)
							AND TYPE_NAME(system_type_id) NOT IN (
								'text'
								,'ntext'
								,'image'
								)
							AND [name] NOT IN (
								@source_table + '_txn'
								,@source_table + '_date'
								,@source_table + '_user'
								,@source_table + '_operation'
								,@source_table + '_expires'
								)
						ORDER BY column_id
						FOR XML path('')
						), 1, 1, '')

			IF NOT EXISTS (
					SELECT 1
					FROM sys.triggers
					WHERE object_id = OBJECT_ID(@source_trigger)
						AND parent_id = OBJECT_ID(@source_qualified)
					)
			BEGIN
				SET @sql = 'CREATE TRIGGER ' + @source_trigger
			END
			ELSE
			BEGIN
				SET @sql = 'ALTER TRIGGER ' + @source_trigger
			END

			SET @sql = @sql + ' ON [' + @source_schema + '].[' + @source_table + '] FOR INSERT, UPDATE, DELETE AS
BEGIN 
	SET NOCOUNT ON
	DECLARE @txn uniqueidentifier = NEWID()	
	DECLARE @expiration_in_days int = (SELECT TOP 1 ISNULL(CAST(value as int),0) AS value FROM sys.extended_properties WITH(NOLOCK) WHERE major_id=' + CAST(OBJECT_ID(@source_qualified) AS NVARCHAR(64)) + ' AND [name] = ''audit.expire_after'')
	DECLARE @expires datetime = ''9999-12-31 23:59:59.997''
	IF @expiration_in_days > 0 BEGIN
		SET @expires = DATEADD(DAY,@expiration_in_days,GETDATE())
	END
	INSERT INTO ' + @destination_qualified + '
	SELECT @txn,GETDATE(),USER_NAME(),''INSERT'',@expires,' + @columns + '
	FROM INSERTED	
	INSERT INTO ' + @destination_qualified + '
	SELECT @txn,GETDATE(),USER_NAME(),''DELETE'',@expires,' + @columns + '
	FROM DELETED	
END
'

			PRINT @sql

			EXECUTE sp_executesql @sql

			COMMIT TRANSACTION
		END TRY

		BEGIN CATCH
			DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
			DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
			DECLARE @ErrorState INT = ERROR_STATE()

			IF @@TRANCOUNT > 0
			BEGIN
				ROLLBACK TRANSACTION
			END

			RAISERROR (
					@ErrorMessage
					,@ErrorSeverity
					,@ErrorState
					)
		END CATCH
	END
	ELSE
	BEGIN
		RAISERROR (
				'Unable to locate destination table %s'
				,10
				,- 1
				,@destination_qualified
				)
	END
END
ELSE
BEGIN
	RAISERROR (
			'Unable to locate source table %s'
			,10
			,- 1
			,@source_qualified
			)
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_alter_audit_on'
	,'#audit'
	,0
	,1
GO

CREATE PROCEDURE [#audit].[sp_create_audit_on] (
	@source_table SYSNAME
	,@source_schema SYSNAME = 'dbo'
	,@destination_table SYSNAME = NULL
	,@destination_schema SYSNAME = 'audit'
	,@expiration_in_days INT = 90
	)
AS
SET NOCOUNT ON

IF @destination_table IS NULL
BEGIN
	SET @destination_table = @source_table
END

DECLARE @source_qualified SYSNAME = '[' + @source_schema + '].[' + @source_table + ']'
DECLARE @destination_qualified SYSNAME = '[' + @destination_schema + '].[' + @destination_table + ']'
DECLARE @sql NVARCHAR(MAX)
DECLARE @columns NVARCHAR(max) = ''

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE object_id = OBJECT_ID(@source_qualified)
		)
BEGIN
	BEGIN TRY
		BEGIN TRANSACTION

		SET @columns = Stuff((
					SELECT ',' + CASE 
							WHEN is_identity = 1
								THEN '0 As [' + [name] + ']'
							ELSE '[' + [name] + ']'
							END
					FROM sys.columns
					WHERE OBJECT_ID = OBJECT_ID(@source_qualified)
						AND TYPE_NAME(system_type_id) NOT IN (
							'text'
							,'ntext'
							,'image'
							)
					ORDER BY column_id
					FOR XML path('')
					), 1, 1, '')
		SET @sql = '
SELECT TOP 1 NEWID() AS [' + @source_table + '_txn],GETDATE() AS [' + @source_table + '_date],USER_NAME() AS [' + @source_table + '_user],''INSERT'' AS [' + @source_table + '_operation],GETDATE() AS [' + @source_table + '_expires],
' + @columns + '
INTO  ' + @destination_qualified + '
FROM ' + @source_qualified + '
WHERE 1=2
'

		PRINT 'Creating audit table ' + @destination_qualified
		PRINT @sql

		EXECUTE sp_executesql @sql

		EXECUTE [audit].[sp_alter_audit_on] @source_table
			,@source_schema
			,@destination_table
			,@destination_schema
			,@expiration_in_days

		COMMIT TRANSACTION
	END TRY

	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
		DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
		DECLARE @ErrorState INT = ERROR_STATE()

		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END

		RAISERROR (
				@ErrorMessage
				,@ErrorSeverity
				,@ErrorState
				)
	END CATCH
END
ELSE
BEGIN
	RAISERROR (
			'Unable to locate source table %s'
			,10
			,- 1
			,@source_qualified
			)
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_create_audit_on'
	,'#audit'
	,0
	,1
GO

CREATE PROCEDURE [#audit].[sp_drop_audit_on] (
	@source_table SYSNAME
	,@source_schema SYSNAME = 'dbo'
	)
AS
SET NOCOUNT ON

DECLARE @source_qualified SYSNAME = '[' + @source_schema + '].[' + @source_table + ']'
DECLARE @destination_qualified AS SYSNAME = '[audit].[' + @source_table + ']'
DECLARE @sql NVARCHAR(MAX)
DECLARE @columns NVARCHAR(max) = ''

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE object_id = OBJECT_ID(@source_qualified)
		)
BEGIN
	BEGIN TRANSACTION

	SELECT @destination_qualified = CAST(value AS NVARCHAR(2048))
	FROM sys.fn_listextendedproperty('audit.destination', 'SCHEMA', @source_schema, 'TABLE', @source_table, DEFAULT, DEFAULT)

	IF @destination_qualified IS NOT NULL
	BEGIN
		DECLARE @source_trigger SYSNAME = '[' + @source_schema + '].[' + @source_table + '_audit]'

		IF EXISTS (
				SELECT 1
				FROM sys.triggers
				WHERE object_id = OBJECT_ID(@source_trigger)
					AND parent_id = OBJECT_ID(@source_qualified)
				)
		BEGIN
			SET @sql = 'DROP TRIGGER ' + @source_trigger

			PRINT 'Dropping audit trigger on ' + @source_qualified
			PRINT @sql

			EXECUTE sp_executesql @sql
		END

		IF EXISTS (
				SELECT 1
				FROM sys.extended_properties
				WHERE major_id = OBJECT_ID(@source_qualified)
					AND [name] = 'audit.destination'
				)
		BEGIN
			PRINT 'Dropping metadata ' + @source_qualified

			EXECUTE sp_dropextendedproperty 'audit.destination'
				,'SCHEMA'
				,@source_schema
				,'TABLE'
				,@source_table
		END

		IF EXISTS (
				SELECT 1
				FROM sys.tables
				WHERE object_id = OBJECT_ID(@destination_qualified)
				)
		BEGIN
			SET @sql = 'DROP TABLE ' + @destination_qualified + ''

			PRINT 'Dropping audit table ' + @destination_qualified

			EXECUTE sp_executesql @sql

			PRINT @sql
		END

		COMMIT TRANSACTION
	END
	ELSE
	BEGIN
		RAISERROR (
				'Unable to locate audit table on %s'
				,10
				,- 1
				,@source_qualified
				)
	END
END
ELSE
BEGIN
	RAISERROR (
			'Unable to locate source table %s'
			,10
			,- 1
			,@source_qualified
			)
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_drop_audit_on'
	,'#audit'
	,0
	,1
GO

CREATE PROCEDURE [#audit].[sp_purge_expired]
AS
SET NOCOUNT ON

DECLARE @queue TABLE (
	[id] INT
	,[schema] SYSNAME
	,[table] SYSNAME
	,value INT
	,[sql] NVARCHAR(MAX)
	)
DECLARE @id INT
DECLARE @sql NVARCHAR(MAX)

INSERT INTO @queue (
	[id]
	,[schema]
	,[table]
	,[value]
	,[sql]
	)
SELECT ROW_NUMBER() OVER (
		ORDER BY t.schema_id
			,t.object_id
		)
	,SCHEMA_NAME(t.schema_id) AS [schema]
	,t.NAME AS [table]
	,CAST(ep.value AS INT) AS [expire_in_days]
	,'DELETE [' + SCHEMA_NAME(t.schema_id) + '].[' + t.NAME + '] WHERE [' + t.NAME + '_expires] < DATEADD(DAY,-' + CAST(ep.value AS NVARCHAR(8)) + ',GETDATE())' AS [sql]
FROM sys.extended_properties ep
INNER JOIN sys.tables t ON t.object_id = ep.major_id
WHERE ep.NAME = 'audit.expire_after'

WHILE EXISTS (
		SELECT 1
		FROM @queue
		)
BEGIN
	SELECT TOP 1 @id = [Id]
		,@sql = [sql]
	FROM @queue

	PRINT 'Executing ' + @sql

	EXECUTE sp_sqlexec @sql

	PRINT CAST(@@ROWCOUNT AS NVARCHAR(64)) + ' rows(s) affected'

	DELETE @queue
	WHERE [Id] = @id
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_purge_expired'
	,'#audit'
	,0
	,1
GO

CREATE PROCEDURE [#audit].[sp_enable_audit_on] (
	@source_table SYSNAME
	,@source_schema SYSNAME = 'dbo'
	)
AS
SET NOCOUNT ON

DECLARE @source_qualified SYSNAME = '[' + @source_schema + '].[' + @source_table + ']'
DECLARE @destination_qualified AS SYSNAME = '[audit].[' + @source_table + ']'
DECLARE @sql NVARCHAR(MAX)

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE object_id = OBJECT_ID(@source_qualified)
		)
BEGIN
	BEGIN TRANSACTION

	SELECT @destination_qualified = CAST(value AS NVARCHAR(2048))
	FROM sys.fn_listextendedproperty('audit.destination', 'SCHEMA', @source_schema, 'TABLE', @source_table, DEFAULT, DEFAULT)

	IF @destination_qualified IS NOT NULL
	BEGIN
		DECLARE @source_trigger SYSNAME = '[' + @source_schema + '].[' + @source_table + '_audit]'

		IF EXISTS (
				SELECT 1
				FROM sys.triggers
				WHERE object_id = OBJECT_ID(@source_trigger)
					AND parent_id = OBJECT_ID(@source_qualified)
				)
		BEGIN
			SET @sql = 'ENABLE TRIGGER ' + @source_trigger +' ON '+ @source_qualified

			PRINT 'Enabling audit trigger on ' + @source_qualified
			PRINT @sql

			EXECUTE sp_executesql @sql
		END

		COMMIT TRANSACTION
	END
	ELSE
	BEGIN
		RAISERROR (
				'Unable to locate audit table on %s'
				,10
				,- 1
				,@source_qualified
				)
	END
END
ELSE
BEGIN
	RAISERROR (
			'Unable to locate source table %s'
			,10
			,- 1
			,@source_qualified
			)
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_enable_audit_on'
	,'#audit'
	,0
	,1
GO 

CREATE PROCEDURE [#audit].[sp_disable_audit_on] (
	@source_table SYSNAME
	,@source_schema SYSNAME = 'dbo'
	)
AS
SET NOCOUNT ON

DECLARE @source_qualified SYSNAME = '[' + @source_schema + '].[' + @source_table + ']'
DECLARE @destination_qualified AS SYSNAME = '[audit].[' + @source_table + ']'
DECLARE @sql NVARCHAR(MAX)

IF EXISTS (
		SELECT 1
		FROM sys.tables
		WHERE object_id = OBJECT_ID(@source_qualified)
		)
BEGIN
	BEGIN TRANSACTION

	SELECT @destination_qualified = CAST(value AS NVARCHAR(2048))
	FROM sys.fn_listextendedproperty('audit.destination', 'SCHEMA', @source_schema, 'TABLE', @source_table, DEFAULT, DEFAULT)

	IF @destination_qualified IS NOT NULL
	BEGIN
		DECLARE @source_trigger SYSNAME = '[' + @source_schema + '].[' + @source_table + '_audit]'

		IF EXISTS (
				SELECT 1
				FROM sys.triggers
				WHERE object_id = OBJECT_ID(@source_trigger)
					AND parent_id = OBJECT_ID(@source_qualified)
				)
		BEGIN
			SET @sql = 'DISABLE TRIGGER ' + @source_trigger +' ON '+ @source_qualified

			PRINT 'Disabling audit trigger on ' + @source_qualified
			PRINT @sql

			EXECUTE sp_executesql @sql
		END

		COMMIT TRANSACTION
	END
	ELSE
	BEGIN
		RAISERROR (
				'Unable to locate audit table on %s'
				,10
				,- 1
				,@source_qualified
				)
	END
END
ELSE
BEGIN
	RAISERROR (
			'Unable to locate source table %s'
			,10
			,- 1
			,@source_qualified
			)
END
GO

EXECUTE sqpkg.sp_register_package_object 'Audit'
	,'sp_disable_audit_on'
	,'#audit'
	,0
	,1
GO 

DROP SCHEMA [#audit]
GO


