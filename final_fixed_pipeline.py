import luigi
import pandas as pd
import re


# Task 1: Renaming Columns
class RenameColumns(luigi.Task):
    input_path = (
        luigi.Parameter()
    )  # adding a parameter as we need this since we have different datasets

    # no requires function since we do not require anything for the first task. no dependencies.

    def output(self):
        return luigi.LocalTarget("aaarenamecolumns.xlsx")

    def run(self):
        dataset = pd.read_excel(self.input_path)
        # all of the datasets have the same column names
        dataset.rename(
            columns={
                "Fecha": "Date",
                "Número": "Number",
                "TPV": "Terminal Point of Sale",
                "Centro": "Center",
                "Ubicación": "Location",
                "Fecha Creación": "Date of Creation",
                "Tipo Doc.": "Type of Document",
                "Cliente": "Client",
                "CIF": "Client ID",
                "Ciudad": "City",
                "Provincia": "Province",
                "Calle": "Street",
                "Código Postal": "Postal Code",
                "Usuario": "Client Name",
                "Tipo Línea": "Manufacturing Line",
                "Grupo Mayor": "Type of Product",
                "Familia": "Product Family",
                "Producto": "Product",
                "Cantidad": "Quantity",
                "Precio": "Price",
                "Dto. %": "Discount in %",
                "Dto. €": "Discount in €",
                "Cód. Promoción": "Promotional Code",
                "Cód. Descuento": "Discount Code",
                "Impuesto %": "Tax in %",
                "Recargo %": "Surcharge in %",
                "Base": "Base Price",
                "Total": "Total Price",
                "Dto. € Ticket": "Ticket Discount in €",
                "Dto. % Ticket": "Ticket Discount in %",
            },
            inplace=True,
        )
        dataset.to_excel(self.output().path, index=False)


# Task 2: removing duplicate rows
class RemovingDuplicates(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return RenameColumns(input_path=self.input_path)

    def output(self):
        return luigi.LocalTarget("aaaremoveduplicates.xlsx")

    def run(self):
        dataset = pd.read_excel(self.input().path)
        noduplicates = dataset.drop_duplicates().reset_index()
        noduplicates.to_excel(self.output().path, index=False)


# task 3: changing the names of the ciudad column
class CiudadColumn(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return RemovingDuplicates(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(
            self.input().path
        )  # does this take the data from the previous task that was run, or just from the input (e.g. barcelona in this case)
        mapping = {
            "bcn": "Barcelona",
            "BCN": "Barcelona",
            "barcelona": "Barcelona",
            "pozuelo de alarcon": "Pozuelo de Alarcon",
            "madrid": "Madrid",
            "santa cruz de tenerife": "Santa Cruz de Tenerife",
            "sevilla": "Sevilla",
            "pontevedra": "Pontevedra",
            "majadahonda": "Majadahona",
            "pozuelo alarcon": "Pozuelo de Alarcon",
            "Urb RIO MONTE _LA NAVATA": "La Navata",
            "alcorcón": "Alcorcón",
            "pozuelo": "Pozuelo de Alarcon",
            "roa": "Roa",
            "pozuelo de Alarcon": "Pozuelo de Alarcon",
            "MADRID": "Madrid",
            "PUERTO DE SAGUNTO": "Puerto de Sagunto",
            "VALENCIA": "Valencia",
            "MANISES": "Manises",
            "SANT CUGAT": "Sant Cugat",
            "PATERNA": "Paterna",
            "Palma de mallorca": "Palma de Mallorca",
            "Palma De Mallorca": "Palma de Mallorca",
            "PALMA DE MALLORCA": "Palma de Mallorca",
            "PALMA": "Palma de Mallorca",
            "cornella de llobregat": "Cornella de Llobregat",
        }
        dataset["City"] = dataset["City"].map(mapping)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaaciudadcolumn.xlsx")


# Task 4: changing the names of the product family
class CleanProductFamily(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return CiudadColumn(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        mapping = {
            "RICE BOWL": "Rice Bowl",
            "EXTRAS": "Extras",
            "NOODLES": "Noodles",
            "COMPRA": "Compra",
            "AGUAS Y REFRESCOS": "Drinks",
            "STARTERS": "Starters",
            "SOUP": "Soup",
            "BAOS": "Bao",
            "BBQ": "BBQ",
            "THAI CURRY'S": "Thai Curry",
            "CERVEZAS": "Alcohol",
            "Vinos y Alcoholes (Contenedor)": "Alcohol",
            "DESSERT": "Dessert",
            "ALCOHOLES": "Alcohol",
            "CAFÉ E INFUSIONES": "Coffee and Tea",
            "Vinos": "Alcohol",
        }
        dataset["Product Family"] = dataset["Product Family"].map(mapping)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaacleanproductfamily.xlsx")


# Task 5: Filling the local column with the mode
class FillLocal(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return CleanProductFamily(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        dataset["Local"] = dataset["Local"].fillna(dataset["Local"].mode()[0])
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaafilllocal.xlsx")


# Task 6: Filling the Tax in % column with 0 when values are none
class FillTen(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return FillLocal(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        dataset["Tax in %"] = dataset["Tax in %"].fillna(
            0
        )  # filling with 0 since if there is a nan value that means that there is no tax
        dataset["Surcharge in %"] = dataset["Surcharge in %"].fillna(0)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaafillten.xlsx")


# Task 7: Changing the type of product columnn values
class ChangeComidaBebida(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return FillTen(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        mapping = {"BEBIDA": "Drinks", "COMIDA": "Food"}
        dataset["Type of Product"] = dataset["Type of Product"].map(mapping)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaachangecomidabebida.xlsx")


# Task 8: Changing the center column valuse
class ChangeDelivery(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return ChangeComidaBebida(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        mapping = {
            "Multi Glovo": "Glovo",
            "Glovo Lunch": "Glovo",
            "Take Away-------- Para recoger": "Other: Take Away",
            "Glovo Catering": "Glovo",
            "Salón": "Not Take Away",
            "Barra": "Not Take Away",
            "Terraza": "Not Take Away",
            "Take Away-------- Para llevar": "Other: Take Away",
            "UberEat": "UberEats",
            "Ubereats": "UberEats",
            "Terraza Valencia": "Not Take Away",
        }
        dataset["Center"] = dataset["Center"].map(mapping)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaachangedelivery.xlsx")


# Task 9: Cleaning the Province Column
class ChangeProvince(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return ChangeDelivery(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)
        mapping = {
            "bcn": "Barcelona",
            "BCN": "Barcelona",
            "barcelona": "Barcelona",
            "Palma de mallorca": "Palma de Mallorca",
            "Palma De Mallorca": "Palma de Mallorca",
            "islas baleares": "Islas Baleares",
            "Baleares": "Islas Baleares",
            "Palma": "Palma de Mallorca",
            "PALMA DE MALLORCA": "Palma de Mallorca",
            "PALMA": "Palma de Mallorca",
            "MADRID": "Madrid",
            "ILLES BALEARES": "Islas Baleares",
            "madrid": "Madrid",
            "santa cruz de tenerife": "Santa Cruz de Tenerife",
            "toledo": "Toledo",
            "burgod": "Burgod",
            "VALENCIA": "Valencia",
            "BARCELONA": "Barcelona",
            "valencia": "Valencia",
        }
        dataset["Province"] = dataset["Province"].map(mapping)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget("aaachangeprovince.xlsx")


# Task 10: Removing the tickmark from the local column
class RemoveTickmark(luigi.Task):
    input_path = luigi.Parameter()

    def requires(self):
        return ChangeProvince(input_path=self.input_path)

    def run(self):
        dataset = pd.read_excel(self.input().path)

        def remove_tickmark(value):
            if pd.isna(value) == True:
                return None
            else:
                return re.sub(" \(✔\)", "", value)

        dataset["Local"] = dataset["Local"].apply(remove_tickmark)
        dataset.to_excel(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            "clean_pozuelo.xlsx"
        )  # this must be changed everytime it is run to match the city in which you are running


if __name__ == "__main__":
    input_path = "datasets/pozuelo.xlsx"  # you must also update the input path to match the file you want to run
    # Specify the input path when creating an instance of RemoveTickmark
    remove_tickmark_task = RemoveTickmark(input_path=input_path)
    luigi.build([remove_tickmark_task], local_scheduler=True)
