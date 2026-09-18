"""
Data Warehouse Hotelero — Esquema Estrella (Star Schema)
Script de configuración, poblado sintético y validación analítica OLAP.
"""

from __future__ import annotations

import random
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from tabulate import tabulate

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent / "hotel_dw.db"
DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"
SEED = 42
DIAS_DIM_TIEMPO = 30
MIN_HECHOS = 50

# Festivos mexicanos (mes, día) — bandera dia_festivo
FESTIVOS_MX = {
    (1, 1),    # Año Nuevo
    (2, 5),    # Día de la Constitución
    (3, 21),   # Natalicio de Benito Juárez
    (5, 1),    # Día del Trabajo
    (5, 5),    # Batalla de Puebla
    (9, 16),   # Independencia
    (11, 2),   # Día de Muertos
    (11, 20),  # Revolución Mexicana
    (12, 12),  # Virgen de Guadalupe
    (12, 25),  # Navidad
}

DIAS_SEMANA = {
    0: "Lunes",
    1: "Martes",
    2: "Miércoles",
    3: "Jueves",
    4: "Viernes",
    5: "Sábado",
    6: "Domingo",
}

HOTELES = [
    {
        "nombre_hotel": "Hotel Presidente InterContinental Ciudad de México",
        "categoria": "5 estrellas",
        "estado": "Ciudad de México",
        "ciudad": "Ciudad de México",
        "municipio": "Miguel Hidalgo",
    },
    {
        "nombre_hotel": "Grand Fiesta Americana Coral Beach Cancún",
        "categoria": "5 estrellas",
        "estado": "Quintana Roo",
        "ciudad": "Cancún",
        "municipio": "Benito Juárez",
    },
    {
        "nombre_hotel": "Hotel Hacienda Los Laureles",
        "categoria": "4 estrellas",
        "estado": "Oaxaca",
        "ciudad": "Oaxaca de Juárez",
        "municipio": "Oaxaca de Juárez",
    },
    {
        "nombre_hotel": "Hotel Casa Blanca Guadalajara",
        "categoria": "4 estrellas",
        "estado": "Jalisco",
        "ciudad": "Guadalajara",
        "municipio": "Guadalajara",
    },
    {
        "nombre_hotel": "Hotel Misión Puebla Centro Histórico",
        "categoria": "3 estrellas",
        "estado": "Puebla",
        "ciudad": "Puebla",
        "municipio": "Puebla",
    },
    {
        "nombre_hotel": "Hotel Quinta Real Monterrey",
        "categoria": "5 estrellas",
        "estado": "Nuevo León",
        "ciudad": "Monterrey",
        "municipio": "San Pedro Garza García",
    },
]

HABITACIONES = [
    {"numero_camas": 1, "internet": True, "tina_hidromasaje": False},
    {"numero_camas": 2, "internet": True, "tina_hidromasaje": False},
    {"numero_camas": 2, "internet": True, "tina_hidromasaje": True},
    {"numero_camas": 3, "internet": True, "tina_hidromasaje": True},
    {"numero_camas": 1, "internet": False, "tina_hidromasaje": False},
]

# Tarifas base por categoría (MXN) para ingresos coherentes
TARIFA_POR_CATEGORIA = {
    "5 estrellas": (3500.0, 8500.0),
    "4 estrellas": (1800.0, 4200.0),
    "3 estrellas": (900.0, 2200.0),
}


# ---------------------------------------------------------------------------
# Fase 1 — Conexión y DDL
# ---------------------------------------------------------------------------
def _enable_foreign_keys(dbapi_conn, _connection_record) -> None:
    """Activa FOREIGN KEY en cada conexión SQLite."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


def create_engine_with_fk(url: str = DATABASE_URL) -> Engine:
    engine = create_engine(url, future=True)
    if url.startswith("sqlite"):
        event.listen(engine, "connect", _enable_foreign_keys)
    return engine


def create_schema(engine: Engine) -> None:
    """
    Crea el esquema estrella.
    Adaptación SQLite:
      - SERIAL → INTEGER PRIMARY KEY AUTOINCREMENT
      - GENERATED ALWAYS AS (... ) STORED (SQLite ≥ 3.31)
    """
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS dim_hotel (
            id_hotel INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_hotel VARCHAR(100),
            categoria VARCHAR(50),
            estado VARCHAR(100),
            ciudad VARCHAR(100),
            municipio VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_tiempo (
            fecha DATE PRIMARY KEY,
            dia_semana VARCHAR(20),
            mes INTEGER,
            año INTEGER,
            dia_festivo BOOLEAN
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_habitacion (
            id_habitacion INTEGER PRIMARY KEY AUTOINCREMENT,
            numero_camas INTEGER,
            internet BOOLEAN,
            tina_hidromasaje BOOLEAN
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS hechos_reservas (
            id_reserva INTEGER PRIMARY KEY AUTOINCREMENT,
            id_hotel INTEGER NOT NULL REFERENCES dim_hotel(id_hotel),
            fecha DATE NOT NULL REFERENCES dim_tiempo(fecha),
            id_habitacion INTEGER NOT NULL REFERENCES dim_habitacion(id_habitacion),
            habitaciones_reservadas INTEGER NOT NULL,
            habitaciones_libres INTEGER NOT NULL,
            habitaciones_no_disponibles INTEGER NOT NULL,
            habitaciones_totales INTEGER
                GENERATED ALWAYS AS (
                    habitaciones_reservadas
                    + habitaciones_libres
                    + habitaciones_no_disponibles
                ) STORED,
            ingresos NUMERIC(10, 2) NOT NULL
        )
        """,
    ]
    with engine.begin() as conn:
        for ddl in ddl_statements:
            conn.execute(text(ddl))


# ---------------------------------------------------------------------------
# Fase 2 — Seed data
# ---------------------------------------------------------------------------
def _es_festivo(d: date) -> bool:
    return (d.month, d.day) in FESTIVOS_MX


def seed_dim_hotel(engine: Engine) -> list[dict]:
    rows = []
    with engine.begin() as conn:
        for hotel in HOTELES:
            result = conn.execute(
                text(
                    """
                    INSERT INTO dim_hotel
                        (nombre_hotel, categoria, estado, ciudad, municipio)
                    VALUES
                        (:nombre_hotel, :categoria, :estado, :ciudad, :municipio)
                    RETURNING id_hotel
                    """
                ),
                hotel,
            )
            id_hotel = result.scalar_one()
            rows.append({"id_hotel": id_hotel, **hotel})
    return rows


def seed_dim_tiempo(engine: Engine, start: date | None = None) -> list[date]:
    """Inserta DIAS_DIM_TIEMPO días consecutivos a partir de start (default: hoy - 15)."""
    if start is None:
        start = date.today() - timedelta(days=15)

    fechas: list[date] = []
    with engine.begin() as conn:
        for offset in range(DIAS_DIM_TIEMPO):
            d = start + timedelta(days=offset)
            conn.execute(
                text(
                    """
                    INSERT INTO dim_tiempo (fecha, dia_semana, mes, año, dia_festivo)
                    VALUES (:fecha, :dia_semana, :mes, :año, :dia_festivo)
                    """
                ),
                {
                    "fecha": d.isoformat(),
                    "dia_semana": DIAS_SEMANA[d.weekday()],
                    "mes": d.month,
                    "año": d.year,
                    "dia_festivo": 1 if _es_festivo(d) else 0,
                },
            )
            fechas.append(d)
    return fechas


def seed_dim_habitacion(engine: Engine) -> list[int]:
    ids: list[int] = []
    with engine.begin() as conn:
        for hab in HABITACIONES:
            result = conn.execute(
                text(
                    """
                    INSERT INTO dim_habitacion
                        (numero_camas, internet, tina_hidromasaje)
                    VALUES
                        (:numero_camas, :internet, :tina_hidromasaje)
                    RETURNING id_habitacion
                    """
                ),
                {
                    "numero_camas": hab["numero_camas"],
                    "internet": 1 if hab["internet"] else 0,
                    "tina_hidromasaje": 1 if hab["tina_hidromasaje"] else 0,
                },
            )
            ids.append(result.scalar_one())
    return ids


def _generar_metricas_habitaciones(rng: random.Random) -> tuple[int, int, int]:
    totales = rng.randint(40, 180)
    no_disponibles = rng.randint(0, max(1, totales // 10))
    disponibles = totales - no_disponibles
    reservadas = rng.randint(int(disponibles * 0.35), disponibles)
    libres = disponibles - reservadas
    return reservadas, libres, no_disponibles


def _calcular_ingresos(
    categoria: str,
    habitaciones_reservadas: int,
    es_festivo: bool,
    rng: random.Random,
) -> float:
    minimo, maximo = TARIFA_POR_CATEGORIA.get(categoria, (1000.0, 3000.0))
    tarifa = rng.uniform(minimo, maximo)
    if es_festivo:
        tarifa *= rng.uniform(1.15, 1.40)
    return round(tarifa * habitaciones_reservadas, 2)


def seed_hechos_reservas(
    engine: Engine,
    hoteles: list[dict],
    fechas: list[date],
    ids_habitacion: list[int],
    n_hechos: int = MIN_HECHOS,
) -> int:
    rng = random.Random(SEED)
    insertados = 0

    with engine.begin() as conn:
        for _ in range(n_hechos):
            hotel = rng.choice(hoteles)
            fecha = rng.choice(fechas)
            id_habitacion = rng.choice(ids_habitacion)
            reservadas, libres, no_disp = _generar_metricas_habitaciones(rng)
            ingresos = _calcular_ingresos(
                hotel["categoria"],
                reservadas,
                _es_festivo(fecha),
                rng,
            )
            conn.execute(
                text(
                    """
                    INSERT INTO hechos_reservas (
                        id_hotel,
                        fecha,
                        id_habitacion,
                        habitaciones_reservadas,
                        habitaciones_libres,
                        habitaciones_no_disponibles,
                        ingresos
                    ) VALUES (
                        :id_hotel,
                        :fecha,
                        :id_habitacion,
                        :habitaciones_reservadas,
                        :habitaciones_libres,
                        :habitaciones_no_disponibles,
                        :ingresos
                    )
                    """
                ),
                {
                    "id_hotel": hotel["id_hotel"],
                    "fecha": fecha.isoformat(),
                    "id_habitacion": id_habitacion,
                    "habitaciones_reservadas": reservadas,
                    "habitaciones_libres": libres,
                    "habitaciones_no_disponibles": no_disp,
                    "ingresos": ingresos,
                },
            )
            insertados += 1
    return insertados


# ---------------------------------------------------------------------------
# Fase 3 — Consultas OLAP
# ---------------------------------------------------------------------------
QUERY_INGRESOS_POR_CIUDAD_HOTEL = """
SELECT
    h.ciudad,
    h.nombre_hotel,
    ROUND(SUM(f.ingresos), 2) AS total_ingresos
FROM hechos_reservas AS f
INNER JOIN dim_hotel AS h ON f.id_hotel = h.id_hotel
GROUP BY h.ciudad, h.nombre_hotel
ORDER BY total_ingresos DESC
"""

QUERY_OCUPACION_FESTIVOS = """
SELECT
    CASE WHEN t.dia_festivo = 1 THEN 'Festivo' ELSE 'No festivo' END AS tipo_dia,
    ROUND(
        AVG(
            CAST(f.habitaciones_reservadas AS REAL)
            / NULLIF(f.habitaciones_totales, 0)
        ) * 100,
        2
    ) AS promedio_ocupacion_pct,
    COUNT(*) AS num_registros
FROM hechos_reservas AS f
INNER JOIN dim_tiempo AS t ON f.fecha = t.fecha
GROUP BY t.dia_festivo
ORDER BY t.dia_festivo DESC
"""

QUERY_DESGLOSE_MENSUAL = """
SELECT
    CAST(t.año AS INTEGER) AS año,
    CAST(t.mes AS INTEGER) AS mes,
    ROUND(SUM(f.ingresos), 2) AS total_ingresos,
    CAST(SUM(f.habitaciones_reservadas) AS INTEGER) AS total_habitaciones_reservadas
FROM hechos_reservas AS f
INNER JOIN dim_tiempo AS t ON f.fecha = t.fecha
GROUP BY t.año, t.mes
ORDER BY t.año, t.mes
"""


def run_query(engine: Engine, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(text(sql), engine)


def print_section(title: str) -> None:
    line = "=" * 72
    print(f"\n{line}")
    print(f"  {title}")
    print(line)


def print_dataframe(df: pd.DataFrame, headers: list[str] | None = None) -> None:
    if df.empty:
        print("  (sin resultados)")
        return
    display = df.copy()
    if headers is not None and len(headers) == len(display.columns):
        display.columns = headers

    # Formato por columna: enteros sin decimales, floats con 2
    table_rows: list[list] = []
    for _, row in display.iterrows():
        formatted: list = []
        for value in row.tolist():
            if isinstance(value, (int,)) and not isinstance(value, bool):
                formatted.append(int(value))
            elif isinstance(value, float):
                if value.is_integer() and abs(value) < 1e12:
                    # Evita 2026.00 cuando pandas trae float desde SQLite
                    formatted.append(int(value))
                else:
                    formatted.append(round(value, 2))
            else:
                formatted.append(value)
        table_rows.append(formatted)

    print(
        tabulate(
            table_rows,
            headers=list(display.columns),
            tablefmt="psql",
            floatfmt=".2f",
        )
    )


def run_olap_validation(engine: Engine) -> None:
    print_section("Consulta 1 — Ingresos acumulados por ciudad y hotel")
    df1 = run_query(engine, QUERY_INGRESOS_POR_CIUDAD_HOTEL)
    print_dataframe(
        df1,
        headers=["Ciudad", "Hotel", "Total ingresos (MXN)"],
    )

    print_section(
        "Consulta 2 — Promedio de ocupación: días festivos vs. no festivos"
    )
    df2 = run_query(engine, QUERY_OCUPACION_FESTIVOS)
    print_dataframe(
        df2,
        headers=["Tipo de día", "Ocupación promedio (%)", "Núm. registros"],
    )

    print_section("Consulta 3 — Desglose mensual de ingresos y habitaciones")
    df3 = run_query(engine, QUERY_DESGLOSE_MENSUAL)
    for col in ("año", "mes", "total_habitaciones_reservadas"):
        if col in df3.columns:
            df3[col] = df3[col].astype(int)
    print_dataframe(
        df3,
        headers=["Año", "Mes", "Total ingresos (MXN)", "Hab. reservadas"],
    )


def verify_integrity(engine: Engine) -> None:
    """Comprueba conteos y que no haya huérfanos en hechos_reservas."""
    checks = {
        "dim_hotel": "SELECT COUNT(*) FROM dim_hotel",
        "dim_tiempo": "SELECT COUNT(*) FROM dim_tiempo",
        "dim_habitacion": "SELECT COUNT(*) FROM dim_habitacion",
        "hechos_reservas": "SELECT COUNT(*) FROM hechos_reservas",
        "fk_huerfanos": """
            SELECT COUNT(*) FROM hechos_reservas AS f
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_hotel h WHERE h.id_hotel = f.id_hotel
            )
            OR NOT EXISTS (
                SELECT 1 FROM dim_tiempo t WHERE t.fecha = f.fecha
            )
            OR NOT EXISTS (
                SELECT 1 FROM dim_habitacion hab
                WHERE hab.id_habitacion = f.id_habitacion
            )
        """,
    }
    print_section("Verificación de integridad referencial")
    with engine.connect() as conn:
        for label, sql in checks.items():
            count = conn.execute(text(sql)).scalar_one()
            print(f"  {label}: {count}")
        # Validar columna generada
        sample = conn.execute(
            text(
                """
                SELECT habitaciones_reservadas, habitaciones_libres,
                       habitaciones_no_disponibles, habitaciones_totales
                FROM hechos_reservas
                LIMIT 1
                """
            )
        ).mappings().one()
        expected = (
            sample["habitaciones_reservadas"]
            + sample["habitaciones_libres"]
            + sample["habitaciones_no_disponibles"]
        )
        assert sample["habitaciones_totales"] == expected, (
            "Columna generada habitaciones_totales inconsistente"
        )
        print("  habitaciones_totales (GENERATED): OK")


# ---------------------------------------------------------------------------
# Orquestación
# ---------------------------------------------------------------------------
def main() -> None:
    # Consolas Windows (cp1252) no soportan todos los caracteres UTF-8
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    print_section("Data Warehouse Hotelero — Setup & Validación OLAP")
    print(f"  Motor: SQLite")
    print(f"  Archivo: {DB_PATH}")
    print(f"  SQLite version: {sqlite3.sqlite_version}")

    if DB_PATH.exists():
        DB_PATH.unlink()

    engine = create_engine_with_fk()

    print_section("Fase 1 — Creación del esquema estrella")
    create_schema(engine)
    print("  Tablas creadas: dim_hotel, dim_tiempo, dim_habitacion, hechos_reservas")

    print_section("Fase 2 — Poblado de datos sintéticos")
    hoteles = seed_dim_hotel(engine)
    print(f"  dim_hotel:        {len(hoteles)} hoteles")

    fechas = seed_dim_tiempo(engine)
    print(f"  dim_tiempo:       {len(fechas)} días ({fechas[0]} a {fechas[-1]})")

    ids_hab = seed_dim_habitacion(engine)
    print(f"  dim_habitacion:   {len(ids_hab)} configuraciones")

    n_hechos = seed_hechos_reservas(engine, hoteles, fechas, ids_hab, MIN_HECHOS)
    print(f"  hechos_reservas:  {n_hechos} registros")

    verify_integrity(engine)

    print_section("Fase 3 — Consultas analíticas OLAP")
    run_olap_validation(engine)

    print_section("Proceso finalizado correctamente")
    engine.dispose()


if __name__ == "__main__":
    main()
