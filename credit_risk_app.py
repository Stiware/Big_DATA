import streamlit as st
import pandas as pd
from databricks import sql
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Page configuration
st.set_page_config(
    page_title="Credit Risk by age",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Credit Risk by age")
st.markdown("---")

# Load data function
@st.cache_data
def load_data():
    from databricks.sdk.runtime import spark
    
    # Query the credit risk data
    df = spark.sql("""
        SELECT 
            person_age,
            loan_grade,
            loan_status,
            loan_amnt,
            loan_int_rate,
            person_income,
            loan_percent_income,
            person_home_ownership,
            loan_intent,
            cb_person_default_on_file
        FROM workspace.bronze.credit_risk_raw
        WHERE person_age > 18 AND person_age < 100
    """).toPandas()
    
    # Create age groups
    df['age_group'] = pd.cut(df['person_age'], 
                             bins=[18, 25, 35, 45, 55, 65, 100],
                             labels=['18-25', '26-35', '36-45', '46-55', '56-65', '65+'])
    
    # Create risk level based on loan grade
    risk_mapping = {'A': 'Bajo', 'B': 'Bajo-Medio', 'C': 'Medio', 
                   'D': 'Medio-Alto', 'E': 'Alto', 'F': 'Alto', 'G': 'Muy Alto'}
    df['risk_level'] = df['loan_grade'].map(risk_mapping)
    
    return df

# Load the data
with st.spinner('Cargando datos...'):
    df = load_data()

# Sidebar filters
st.sidebar.header("🔍 Filtros")

selected_age_groups = st.sidebar.multiselect(
    "Grupos de Edad",
    options=df['age_group'].unique().tolist(),
    default=df['age_group'].unique().tolist()
)

selected_risk_levels = st.sidebar.multiselect(
    "Niveles de Riesgo",
    options=df['risk_level'].dropna().unique().tolist(),
    default=df['risk_level'].dropna().unique().tolist()
)

# Filter data
filtered_df = df[
    (df['age_group'].isin(selected_age_groups)) &
    (df['risk_level'].isin(selected_risk_levels))
]

# Key Metrics
st.header("📈 Métricas Clave")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Aplicaciones",
        f"{len(filtered_df):,}",
        delta=None
    )

with col2:
    default_rate = (filtered_df['loan_status'].sum() / len(filtered_df) * 100)
    st.metric(
        "Tasa de Default",
        f"{default_rate:.2f}%",
        delta=None
    )

with col3:
    avg_loan = filtered_df['loan_amnt'].mean()
    st.metric(
        "Préstamo Promedio",
        f"${avg_loan:,.0f}",
        delta=None
    )

with col4:
    avg_age = filtered_df['person_age'].mean()
    st.metric(
        "Edad Promedio",
        f"{avg_age:.1f} años",
        delta=None
    )

st.markdown("---")

# Risk Analysis by Age Group
st.header("🎯 Análisis de Riesgo por Grupo de Edad")

col1, col2 = st.columns(2)

with col1:
    # Default rate by age group
    age_risk = filtered_df.groupby('age_group').agg({
        'loan_status': ['sum', 'count']
    }).reset_index()
    age_risk.columns = ['age_group', 'defaults', 'total']
    age_risk['default_rate'] = (age_risk['defaults'] / age_risk['total'] * 100)
    
    fig1 = px.bar(
        age_risk,
        x='age_group',
        y='default_rate',
        title='Tasa de Default por Grupo de Edad',
        labels={'age_group': 'Grupo de Edad', 'default_rate': 'Tasa de Default (%)'},
        color='default_rate',
        color_continuous_scale='Reds'
    )
    fig1.update_layout(showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # Risk level distribution by age
    risk_dist = filtered_df.groupby(['age_group', 'risk_level']).size().reset_index(name='count')
    
    fig2 = px.bar(
        risk_dist,
        x='age_group',
        y='count',
        color='risk_level',
        title='Distribución de Niveles de Riesgo por Edad',
        labels={'age_group': 'Grupo de Edad', 'count': 'Cantidad', 'risk_level': 'Nivel de Riesgo'},
        barmode='stack',
        color_discrete_map={
            'Bajo': '#2ecc71',
            'Bajo-Medio': '#3498db',
            'Medio': '#f39c12',
            'Medio-Alto': '#e67e22',
            'Alto': '#e74c3c',
            'Muy Alto': '#c0392b'
        }
    )
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# Detailed Analysis
st.header("📊 Análisis Detallado por Edad")

col1, col2 = st.columns(2)

with col1:
    # Average loan amount by age
    avg_loan_age = filtered_df.groupby('age_group')['loan_amnt'].mean().reset_index()
    
    fig3 = px.line(
        avg_loan_age,
        x='age_group',
        y='loan_amnt',
        title='Monto Promedio de Préstamo por Edad',
        labels={'age_group': 'Grupo de Edad', 'loan_amnt': 'Monto Promedio ($)'},
        markers=True
    )
    fig3.update_traces(line_color='#3498db', line_width=3)
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    # Average interest rate by age and risk
    avg_rate = filtered_df.groupby(['age_group', 'risk_level'])['loan_int_rate'].mean().reset_index()
    
    fig4 = px.box(
        filtered_df,
        x='age_group',
        y='loan_int_rate',
        color='risk_level',
        title='Tasa de Interés por Edad y Nivel de Riesgo',
        labels={'age_group': 'Grupo de Edad', 'loan_int_rate': 'Tasa de Interés (%)', 'risk_level': 'Nivel de Riesgo'}
    )
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# Summary Table
st.header("📋 Tabla Resumen por Grupo de Edad")

summary = filtered_df.groupby('age_group').agg({
    'loan_status': ['count', 'sum'],
    'loan_amnt': 'mean',
    'loan_int_rate': 'mean',
    'person_income': 'mean',
    'loan_percent_income': 'mean'
}).round(2)

summary.columns = ['Total Aplicaciones', 'Defaults', 'Préstamo Promedio ($)', 
                   'Tasa Interés Prom (%)', 'Ingreso Promedio ($)', 'Préstamo/Ingreso (%)']
summary['Tasa Default (%)'] = (summary['Defaults'] / summary['Total Aplicaciones'] * 100).round(2)

st.dataframe(summary, use_container_width=True)

# Risk characterization
st.markdown("---")
st.header("🔍 Caracterización de Riesgo")

col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Bajo Riesgo (A-B)")
    low_risk = filtered_df[filtered_df['loan_grade'].isin(['A', 'B'])]
    st.write(f"**Edad promedio:** {low_risk['person_age'].mean():.1f} años")
    st.write(f"**Default rate:** {(low_risk['loan_status'].sum()/len(low_risk)*100):.2f}%")
    st.write(f"**Monto promedio:** ${low_risk['loan_amnt'].mean():,.0f}")

with col2:
    st.subheader("Riesgo Medio (C-D)")
    med_risk = filtered_df[filtered_df['loan_grade'].isin(['C', 'D'])]
    st.write(f"**Edad promedio:** {med_risk['person_age'].mean():.1f} años")
    st.write(f"**Default rate:** {(med_risk['loan_status'].sum()/len(med_risk)*100):.2f}%")
    st.write(f"**Monto promedio:** ${med_risk['loan_amnt'].mean():,.0f}")

with col3:
    st.subheader("Alto Riesgo (E-G)")
    high_risk = filtered_df[filtered_df['loan_grade'].isin(['E', 'F', 'G'])]
    if len(high_risk) > 0:
        st.write(f"**Edad promedio:** {high_risk['person_age'].mean():.1f} años")
        st.write(f"**Default rate:** {(high_risk['loan_status'].sum()/len(high_risk)*100):.2f}%")
        st.write(f"**Monto promedio:** ${high_risk['loan_amnt'].mean():,.0f}")
    else:
        st.write("No hay datos para este segmento")

# Footer
st.markdown("---")
st.caption("📊 Dashboard de Análisis de Riesgo Crediticio - Databricks App")