import psycopg2
import json
from datetime import datetime

def generate_recommendations_from_db(db_connection):
    """
    Generate energy optimization recommendations based on sensor data and predictions.
    Stores recommendations in the recommendations table.
    """
    try:
        # Connect to the database
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

        # Fetch recent sensor data and predictions (last 24 hours)
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
        data = cur.fetchall()

        recommendations = []
        for row in data:
            timestamp, building_id, energy, occupancy, power_factor, temperature, predicted_energy, anomaly = row
            recommendation = {}

            # Generate recommendations based on data
            if anomaly and "power_factor_abnormal" in anomaly:
                recommendation["description"] = "Optimize power factor: Consider adding power factor correction capacitors."
                recommendation["priority"] = "high"
            elif energy > predicted_energy * 1.2:  # If actual energy exceeds predicted by 20%
                recommendation["description"] = "Reduce energy usage: Schedule high-energy equipment during off-peak hours."
                recommendation["priority"] = "medium"
            elif occupancy < 10 and energy > 50:  # Low occupancy but high energy
                recommendation["description"] = "Reduce HVAC and lighting: Low occupancy detected."
                recommendation["priority"] = "medium"
            elif temperature > 26:  # High temperature
                recommendation["description"] = "Adjust HVAC: Temperature exceeds 26°C, consider cooling optimization."
                recommendation["priority"] = "low"
            else:
                recommendation["description"] = "No action needed: Energy usage within expected range."
                recommendation["priority"] = "low"

            recommendations.append((
                timestamp,
                building_id,
                predicted_energy if predicted_energy else 0,
                json.dumps(recommendation)
            ))

        # Insert recommendations into the table
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
