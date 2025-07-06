# OptimizeEnergy/optimize_energy.py
import psycopg2
import json
from datetime import datetime

def generate_recommendations_from_db(db_connection):
    """
    Generate energy optimization recommendations based on sensor data and predictions.
    Stores recommendations in the recommendations table.
    """
    try:
        conn = psycopg2.connect(db_connection)
        cur = conn.cursor()

        # Ensure the recommendations table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                timestamp TIMESTAMP NOT NULL,
                building_id INT NOT NULL,
                predicted_energy DOUBLE PRECISION,
                recommendation JSONB,
                PRIMARY KEY (timestamp, building_id)
            );
        """)

        # Fetch recent sensor and prediction data
        cur.execute("""
            SELECT 
                s.timestamp, 
                s.building_id, 
                s.energy, 
                s.occupancy, 
                s.power_factor, 
                s.temperature, 
                p.predicted_energy, 
                p.anomaly
            FROM sensor_data s
            LEFT JOIN predictions p 
                ON s.timestamp = p.timestamp 
                AND s.building_id = p.building_id
            WHERE s.timestamp >= NOW() - INTERVAL '24 hours'
            ORDER BY s.timestamp, s.building_id;
        """)
        rows = cur.fetchall()

        recommendations = []

        for row in rows:
            timestamp, building_id, energy, occupancy, power_factor, temperature, predicted_energy, anomaly = row
            recommendation = {}

            if anomaly and "power_factor_abnormal" in anomaly:
                recommendation = {
                    "description": "Optimize power factor: Consider adding power factor correction capacitors.",
                    "priority": "high",
                    "tag": "power_factor"
                }
            elif predicted_energy and energy > predicted_energy * 1.2:
                recommendation = {
                    "description": "Reduce energy usage: Schedule high-energy equipment during off-peak hours.",
                    "priority": "medium",
                    "tag": "energy_spike"
                }
            elif occupancy < 5 and energy > 30:
                recommendation = {
                    "description": "Reduce HVAC and lighting: Low occupancy detected with high usage.",
                    "priority": "medium",
                    "tag": "occupancy_mismatch"
                }
            elif temperature > 26:
                recommendation = {
                    "description": "Adjust HVAC: Temperature exceeds 26°C, consider cooling optimization.",
                    "priority": "low",
                    "tag": "temperature_control"
                }
            else:
                recommendation = {
                    "description": "No immediate action required: Conditions within optimal range.",
                    "priority": "low",
                    "tag": "normal"
                }

            recommendations.append((
                timestamp,
                building_id,
                predicted_energy if predicted_energy else 0.0,
                json.dumps(recommendation)
            ))

        # Insert or update recommendations
        if recommendations:
            cur.executemany("""
                INSERT INTO recommendations (timestamp, building_id, predicted_energy, recommendation)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (timestamp, building_id) DO UPDATE
                  SET predicted_energy = EXCLUDED.predicted_energy,
                      recommendation = EXCLUDED.recommendation
            """, recommendations)

        conn.commit()
        cur.close()
        conn.close()

        return len(recommendations)

    except Exception as e:
        raise Exception(f"Error generating recommendations: {str(e)}")
