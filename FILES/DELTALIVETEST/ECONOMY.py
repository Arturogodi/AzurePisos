# Databricks notebook source
class ProcesadorTransacciones:
    def __init__(self, df):
        self.df = df

    @staticmethod
    def transformar_fecha(df):
        df['fecha_transaccion'] = pd.to_datetime(df['fecha_transaccion'])
        return df

    @staticmethod
    def calcular_total(df):
        df['total_transacciones'] = df.groupby('id_cliente')['monto'].transform('sum')
        return df

    def procesar(self):
        self.df = self.transformar_fecha(self.df)
        self.df = self.calcular_total(self.df)
        return self.df

