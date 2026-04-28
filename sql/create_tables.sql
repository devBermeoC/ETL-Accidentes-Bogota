-- ===========================================
-- Base de datos : Accidentes Bogota
-- Descripción: Accidentes de tráfico Bogota
-- Periodo: 2007 - 2017
-- ===========================================

-- Crear base de datos

IF NOT EXISTS ( SELECT name FROM sys.databases WHERE name = 'accidentes_bogota')
BEGIN
    CREATE DATABASE accidentes_bogota;
END
GO

USE accidentes_bogota;
GO

-- Eliminación de tabla existente para crearla nuevamente limpia
IF OBJECT_ID('dbo.accidentes','U') IS NOT NULL
    DROP TABLE dbo.accidentes;
GO

-- Crear tabla principal

CREATE TABLE dbo.accidentes(
    id                INT IDENTITY(1,1) PRIMARY KEY,
    objectid          BIGINT            NOT NULL,
    codigo_accidente  VARCHAR(20)       NOT NULL,
    fecha_ocurrencia  DATE              NOT NULL,
    hora_ocurrencia   TIME              NOT NULL,
    ano_ocurrencia    SMALLINT          NOT NULL,
    mes_ocurrencia    VARCHAR(15)       NOT NULL,
    dia_ocurrencia    VARCHAR(15)       NOT NULL,
    direccion         VARCHAR(100)      NOT NULL,
    gravedad          VARCHAR(30)       NOT NULL,
    clase             VARCHAR(30)       NOT NULL,
    localidad         VARCHAR(50)       NOT NULL,
    municipio         VARCHAR(50)       NOT NULL,
    latitud           DECIMAL(15,10)    NULL,
    longitud          DECIMAL(15,10)    NULL
);

-- 'Indices para mejorar consultas frecuentes
CREATE INDEX idx_fecha ON dbo.accidentes(fecha_ocurrencia);
CREATE INDEX idx_localidad ON dbo.accidentes(localidad);
CREATE INDEX idx_gravedad ON dbo.accidentes(gravedad);
CREATE INDEX idx_clase ON dbo.accidentes(clase);
GO

USE accidentes_bogota;
SELECT TOP 10 * FROM dbo.accidentes;
SELECT COUNT(*) AS total FROM dbo.accidentes;


