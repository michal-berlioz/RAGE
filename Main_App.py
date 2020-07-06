import tkinter as tk
from tkinter import filedialog, IntVar
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Text, Float, Integer
import numpy as np
from openpyxl import load_workbook
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine, String, Column, Table, MetaData
from sklearn import preprocessing, neighbors, model_selection

class MainApp:
    def file_import(self):  # funkcja importująca plik źródłowy
        self.path = filedialog.askopenfilename(filetypes=(
            ("Excel files", ".xlsx .xls"), ("all_files", "*.*"), ("xlsx", "*.xlsx"), ("xls", "*.xls")))
        self.main_tab = pd.read_excel(self.path, header=None)
        self.belka = tk.Label(text=f"uploaded file: {self.path}", anchor="w", bg='light yellow')
        self.belka.place(x=260, y=30, height=30, width=500)

    def file_export(self):

        def source_file_processing(main_tab, path):  # obróbka pliku źródłowegoo
            header_row = main_tab[3].first_valid_index()  # znajduję pierwszy nienullowy wiersz
            del main_tab
            main_tab = pd.read_excel(path, header=header_row)  # wczytuję ponownie od nienullowego wiersza
            main_tab.dropna(thresh=len(main_tab.columns) - 4, inplace=True)  # usuwam stopkę - częściowo pusty wiersz
            ind = np.where(main_tab.columns.str.contains('^godz'))  # znalezienie indeksu nagłówka do zmiany
            main_tab.rename(columns={main_tab.columns[ind[0][0]]: 'godzina'}, inplace=True)  # zamiany nazw nagłówków
            ind = np.where(main_tab.columns.str.contains('^GRP|grp'))
            main_tab.rename(columns={main_tab.columns[ind[0][0]]: 'GRP'}, inplace=True)
            ind = np.where(main_tab.columns.str.contains('^kana'))
            main_tab.rename(columns={main_tab.columns[ind[0][0]]: 'Channel'}, inplace=True)
            main_tab.godzina = main_tab.godzina.astype(int)  # Zmiana formatu godziny
            main_tab.drop(main_tab.filter(regex='Unnamed'), axis=1, inplace=True)
            return main_tab

        def target_selecting(columns):  # wybór grupy referencyjnej
            root_b = tk.Toplevel()
            root_b.wm_attributes('-topmost', 1)
            app_target = tk.StringVar()  # inicjalizacja zmiennej TKINTER
            app_target.set(columns[2])  # ustawienie wartości zmiennej
            lab = tk.Label(root_b, text='Select a reference target')
            lab.pack()
            columns[2:]
            for text in columns[2:]:
                tk.Radiobutton(root_b, text=text, variable=app_target, value=text,
                               command=lambda: app_target.set(app_target.get())).pack(anchor="w")
            exit_button = tk.Button(root_b, text='Potwierdzam to', command=root_b.destroy)
            exit_button.pack()
            root_b.wait_window()
            selected_target = app_target.get()
            return selected_target

        def indexing(grp_tab, selected_target):  # tworzenie indeksów grup na podstawie danych z NIELSENA
            temp_ind = grp_tab.groupby(
                ['Channel']).sum()  # tabelka pomocnicza do indeksów dobowych(do wypełniania braków)
            temp_ind.drop(columns='godzina', inplace=True)
            target_list = []
            for i in grp_tab.columns[2:]:
                if i != selected_target:
                    new_col = 'ind_' + i
                    aux_temp_col = 'aux_ind_' + i
                    grp_tab[new_col] = grp_tab[i] / grp_tab[selected_target]
                    temp_ind[aux_temp_col] = temp_ind[i] / temp_ind[selected_target]
                    grp_tab = pd.merge(grp_tab, temp_ind[[aux_temp_col]], right_index=True, on='Channel')
                    grp_tab[new_col].fillna(grp_tab[aux_temp_col], inplace=True)
                    target_list.append(i)
            return grp_tab, target_list

        def slownik_import():  # import słowników
            src_slownik = r"C:\Users\Michał\Documents\tabele\slownik_zw.xlsx"
            slownik_channels = pd.read_excel(src_slownik, sheet_name='channels')
            slownik_grp = pd.read_excel(src_slownik, header=None, sheet_name='Temp_GRP')
            header_row = slownik_grp[slownik_grp.iloc[:, 0] == 'Channel'].index[0]
            df_temp = slownik_grp.iloc[:, 1]
            df_temp = df_temp[df_temp.str.contains("Universe") == True]
            df_temp.reset_index(drop=True, inplace=True)
            targets_tab = pd.DataFrame()
            targets_tab['Target'] = df_temp.str.split(r"\[|Universe: ", expand=True)[0]
            targets_tab['Universe'] = df_temp.str.split("Universe: ", expand=True)[1].str.rsplit("Cases:", expand=True)[
                0]
            # targets_tab['Target'] = df_temp[0].str.rstrip()
            targets_tab['Universe'] = targets_tab['Universe'].str.replace(u"\xa0", "")
            targets_tab['Target'] = targets_tab['Target'].str.replace(u"\xa0", "")
            targets_tab['Target'] = targets_tab['Target'].astype(str)
            targets_tab['Target'] = targets_tab['Target'].str.rstrip()
            targets_tab['Universe'] = targets_tab['Universe'].astype(int)
            targets_tab = targets_tab.transpose()
            targets_tab.columns = targets_tab.iloc[0, :]
            targets_tab.drop(['Target'], inplace=True)
            targets_tab = targets_tab.astype(int)
            del slownik_grp
            slownik_grp = pd.read_excel(src_slownik, header=header_row, sheet_name='Temp_GRP')
            return slownik_channels, slownik_grp, targets_tab

        def slownik_processing(slownik_grp, slownik_channels):  # obróbka słowników
            slownik_grp.rename(columns={'Day part\Target': 'godzina'}, inplace=True)
            slownik_grp['Channel'] = slownik_grp['Channel'].map(slownik_channels.set_index('arianna_name')['sobr_name'])
            # zamiana nazw ariannowych na sobrowe
            left = slownik_grp['godzina'].str[:2]  # wyłuskanie godzin z ariannowej nazwy pasma
            slownik_grp['godzina'] = pd.to_numeric(left)
            selected_target = target_selecting(slownik_grp.columns)
            print('Reference Target is: ' + selected_target)
            slownik_grp, target_list = indexing(slownik_grp, selected_target)
            return slownik_grp, target_list, selected_target

        def data_merging(main_tab, slownik_channels, slownik_grp, target_list, selected_target,
                         targets_tab):
            main_tab['channel_group'] = main_tab['Channel'].map(
                slownik_channels.set_index('sobr_name')['group_name'])  # left join w inny sposób
            main_tab['kod'] = main_tab['godzina'].map(str) + main_tab[
                'Channel']  # tworzenie identyfikatora(klucz główny) do łączenia indeksu
            slownik_grp['kod'] = slownik_grp['godzina'].astype(str) + slownik_grp[
                'Channel']  # identyfikator w drugiej tabeli
            grp_abs_col = 'liczba_kontaktów_' + selected_target
            main_tab[grp_abs_col] = round(main_tab['GRP'] / 100 * targets_tab[selected_target][0], 0).astype(int)
            main_tab[grp_abs_col].fillna(0, inplace=True)
            for i in target_list:
                index_column = 'ind_' + i
                grp_abs_col = 'liczba_kontaktów_' + i
                aux_temp_col = 'aux_ind_' + i
                grp_col = 'GRP_' + i
                main_tab[index_column] = main_tab['kod'].map(
                    slownik_grp.set_index('kod')[index_column])  # kolumna z indeksami poprzez zmapowanie słownika
                aux_index_pd = slownik_grp.set_index('Channel')[
                    aux_temp_col].drop_duplicates()  # tymczasowa tabela do przypisywania indeksó dobowych w celu uzupełnienia braków
                main_tab[index_column] = main_tab[index_column].fillna(
                    main_tab['Channel'].map(aux_index_pd))  # braki zmapowane tabelą pomocniczą
                main_tab[grp_col] = main_tab['GRP'] * main_tab[index_column]  # indeks * GRP grupy referencyjnej
                main_tab.drop(columns=[index_column], inplace=True)
                main_tab[grp_abs_col] = round(main_tab[grp_col] / 100 * targets_tab[i][0], 0).astype(int)
                main_tab[grp_abs_col].fillna(0, inplace=True)
            main_tab.drop(columns='kod', inplace=True)
            return main_tab

        def output_file_export(tab):
            root_c = tk.Toplevel()
            root_c.wm_attributes('-topmost', 1)
            belka_3 = tk.Label(root_c, text="Name your output file:")
            belka_3.pack(side="left")
            vartext = tk.StringVar()
            input_window = tk.Entry(root_c, textvariable=vartext, width=50)
            input_window.pack(side="left")
            exit_c = tk.Button(root_c, text='OK', command=root_c.destroy)
            exit_c.pack(side="left")
            # root_c.mainloop()
            root_c.wait_window()
            name = vartext.get()
            link_out = r"C:/Users/Michał/Desktop/{}.xlsx".format(name)
            tab.to_excel(link_out, index=False)
            return link_out



        self.main_tab = source_file_processing(self.main_tab, self.path)
        self.slownik_channels, self.slownik_grp, self.targets_tab = slownik_import()
        self.slownik_grp, self.target_list, self.selected_target = slownik_processing(self.slownik_grp,
                                                                                      self.slownik_channels)
        self.main_tab = data_merging(self.main_tab, self.slownik_channels, self.slownik_grp, self.target_list,
                                     self.selected_target, self.targets_tab)

        self.lok = output_file_export(self.main_tab)
        self.belka2 = tk.Label(text=f"exported file: {self.lok}", anchor="w", bg='light yellow')
        self.belka2.place(x=260, y=70, height=30, width=500)

    def update_db(self):
        def csv_import(link):
            tab = pd.read_csv(link, decimal=',', delimiter=';', header=2)
            tab.dropna(axis=1, thresh=3, inplace=True)
            # tab.drop(columns=[tab.columns.values[len(tab.columns)-1]], inplace=True)
            src_slownik = r"C:\Users\Michał\Documents\tabele\slownik_zw.xlsx"
            slownik_channels = pd.read_excel(src_slownik, sheet_name='channels')
            tab['Channel'] = tab['Channel'].map(slownik_channels.set_index('arianna_name')['sobr_name'])
            tab.rename(columns={r'Start Time\Variables': 'time', 'Cumulated Reach%': 'reach_1+', 'Freq. 3+': 'reach_3+'},
                       inplace=True)
            tab['reach_1+'] = tab['reach_1+'].str.replace(" %", "")
            tab['reach_1+'] = tab['reach_1+'].str.replace(",", ".").astype(float)
            tab['reach_3+'] = tab['reach_3+'].str.replace(" %", "")
            tab['reach_3+'] = tab['reach_3+'].str.replace(",", ".").astype(float)
            tab['reach_1+'] = tab['reach_1+'] / 100
            tab['reach_3+'] = tab['reach_3+'] / 100
            tab['hour'] = tab.apply(lambda t: int(t.time[0:2]), axis=1)
            tab = tab.assign(daypart=pd.cut(tab.hour, [0, 17, 22, 29], labels=["off", "prime", "off2"]))
            tab.daypart = tab.daypart.str.replace('off2', 'off')

            aux_campaigns = pd.DataFrame(tab['Producer'].drop_duplicates())
            aux_campaigns['id'] = range(1, len(aux_campaigns) + 1)
            aux_campaigns['id'] = aux_campaigns['id'].astype(str)

            tab['campaign_id'] = tab['Producer'].map(aux_campaigns.set_index('Producer')['id'])
            tab_grp = pd.pivot_table(tab, index=["campaign_id", "Target"], columns=['Channel', 'daypart'], values="GRP",
                                     aggfunc='sum')
            col_names = ['_'.join(tups) for tups in list(tab_grp.columns)]
            tab_grp.columns = pd.Index(col_names)
            tab_days = pd.pivot_table(tab, index=["campaign_id", "Target"], values="Date", aggfunc=pd.Series.nunique)
            tab_r1 = pd.pivot_table(tab, index=["campaign_id", "Target"], columns="daypart", values="reach_1+",
                                    aggfunc='max')
            tab_r1.columns = pd.Index(list(tab_r1.columns))  # KOLUMNY TUTAJ PRZESTAJĄ MIEĆ INDEXY KATEGORYZOWANE
            tab_r1['reach_1+'] = tab_r1.max(axis=1)
            tab_r1 = tab_r1.drop(tab_r1.columns[[0, 1]], axis=1)
            tab_r3 = pd.pivot_table(tab, index=["campaign_id", "Target"], columns="daypart", values="reach_3+",
                                    aggfunc='max')
            tab_r3.columns = pd.Index(list(tab_r3.columns))  # KOLUMNY TUTAJ PRZESTAJĄ MIEĆ INDEXY KATEGORYZOWANE
            tab_r3['reach_3+'] = tab_r3.max(axis=1)
            tab_r3 = tab_r3.drop(tab_r3.columns[[0, 1]], axis=1)
            vectors = pd.concat([tab_grp, tab_days, tab_r1, tab_r3], axis=1)
            vectors.fillna(0, inplace=True)
            vectors = vectors.reset_index()
            vectors.drop(columns='campaign_id', inplace=True)
            # row_names = ['_'.join(tups) for tups in list(vectors.index)]
            # vectors.index = pd.Index(row_names)
            vectors.rename(columns={'Date': 'Days'}, inplace=True)
            row_numbers = len(vectors)
            return vectors, row_numbers

        def connect_to_database():
            engine = create_engine('mysql+mysqlconnector://root:nasa12crew@localhost:3306/DB_Campaigns')
            return engine

        def export_vectors_to_db(vectors, engine):
            col_names = list(vectors.columns)
            types_dict = {new_list: Float for new_list in col_names}
            types_dict['Target'] = Text
            types_dict['Days'] = Integer
            types_dict['reach_1+'] = Float
            types_dict['reach_3+'] = Float
            vectors.to_sql(
                'campaigns',
                engine,
                if_exists='append',
                index=False,
                dtype=types_dict
            )
        link = r"C:\Users\Michał\Documents\tabele\training_set_2.csv"
        self.vectors = csv_import(link)
        self.engine = connect_to_database()
        export_vectors_to_db(self.vectors[0], self.engine)
        self.belka2 = tk.Label(text=f"added: {self.vectors[1]} rows", anchor="w", bg='light yellow')
        self.belka2.place(x=260, y=190, height=30, width=500)
        self.vectors = None

    def estimate_reach(self):

        def get_camp_vector(main_tab):
            new_tab = main_tab.assign(daypart=pd.cut(main_tab.godzina, [0, 17, 22, 29], labels=["off", "prime", "off2"]))
            new_tab.daypart = new_tab.daypart.str.replace('off2', 'off')
            new_tab = new_tab.assign(temp_id=1)
            vector = pd.pivot_table(new_tab, index='temp_id', columns=['Channel', 'daypart'], values='GRP', aggfunc='sum')
            col_names = ['_'.join(tups) for tups in list(vector.columns)]
            vector.columns = pd.Index(col_names)
            ind = np.where(new_tab.columns.str.contains('^Dat|dat'))
            new_tab.rename(columns={new_tab.columns[ind[0][0]]: 'Days'}, inplace=True)
            days_number = pd.pivot_table(new_tab, index='temp_id', values="Days", aggfunc=pd.Series.nunique)
            vector = pd.concat([vector, days_number], axis=1, join='outer')
            return vector

        def see_available_targets():
            base = automap_base()
            engine = create_engine('mysql+mysqlconnector://root:nasa12crew@localhost:3306/DB_Campaigns')
            base.prepare(engine, reflect=True)
            campaigns = base.classes.campaigns
            # session = Session(engine)
            session = sessionmaker(bind=engine)()
            # result = session.query(campaigns).all()
            # result = [r.Days for r in session.query(campaigns).all()]
            meta = MetaData()
            # camps = Table('campaigns', meta, autoload=True, autoload_with=engine)
            query = session.query(campaigns.target)
            vectors = pd.read_sql(query.statement, query.session.bind)
            params = vectors['target'].unique()
            return params

        def params_selecting(params):  # wybór grupy referencyjnej
            root_d = tk.Toplevel()
            root_d.wm_attributes('-topmost', 1)
            lab = tk.Label(root_d, text='Select targets')
            lab.pack()
            var_dict = {}
            i = 0
            for param in params:
                var_dict[i] = IntVar()
                tk.Checkbutton(root_d, text=param, variable=var_dict[i]).pack()
                i += 1
            exit_button = tk.Button(root_d, text='Potwierdzam to', command=root_d.destroy)
            exit_button.pack()
            root_d.wait_window()
            i = 0
            selected_params = []
            for param in params:
                if var_dict[i].get() == 1:
                    selected_params.append(param)
                i += 1
            return selected_params

        def get_vectors_from_db(selected_params):
            Base = automap_base()
            engine = create_engine('mysql+mysqlconnector://root:nasa12crew@localhost:3306/DB_Campaigns')
            Base.prepare(engine, reflect=True)
            campaigns = Base.classes.campaigns
            session = Session(engine)
            session = sessionmaker(bind=engine)()
            result = session.query(campaigns).all()
            result = [r.Days for r in session.query(campaigns).all()]
            targets = session.query(campaigns).filter(campaigns.target.in_(selected_params))
            targets = session.query(campaigns).all()
            meta = MetaData()
            camps = Table('campaigns', meta, autoload=True, autoload_with=engine)
            query = session.query(campaigns).filter(campaigns.target.in_(selected_params))
            vectors = pd.read_sql(query.statement, query.session.bind)
            # for t in targets:
            #     print(t.target)
            # print(len(targets.Days))
            return vectors

        def estimate_reach(ref_tg, selected_params, endo_vector, exo_vectors):
            temp_x = exo_vectors.drop(columns=['reach_1+', 'reach_3+', 'target', 'id'])
            endo_vector = pd.concat([temp_x, endo_vector], sort=True)
            endo_vector = endo_vector.tail(1)
            endo_vector = endo_vector.fillna(0)
            reach1 = pd.DataFrame
            reach3 = pd.DataFrame
            x = exo_vectors.loc[exo_vectors[ref_tg].isin(ref_tg)].drop(columns=['reach_1+', 'reach_3+'])
            for target in selected_params:
                exo_vectors_tg = exo_vectors.loc[exo_vectors['target'].isin(target)]
                y1 = exo_vectors_tg[['id', 'reach_1+']]
                y2 = exo_vectors_tg[['id', 'reach_3+']]
                
                model_r1 = neighbors.KNeighborsRegressor(3, metric='euclidean')
                model_r1.fit(x, y1)
                r1 = model_r1.predict(endo_vector)

                model_r3 = neighbors.KNeighborsRegressor(3, metric='euclidean')
                model_r3.fit(x, y2)
                r3 = model_r1.predict(endo_vector)
                reach1.target = r1
                reach3.target = r3
            return reach1, reach3

        self.endo_vector = get_camp_vector(self.main_tab)
        self.params = see_available_targets()
        self.selected_params = params_selecting(self.params)
        self.exo_vectors = get_vectors_from_db(self.selected_params)
        self.reach1, self.reach3 = estimate_reach(self.selected_target, self.selected_params, self.endo_vector, self.exo_vectors)


        src_slownik = r"C:\Users\Michał\Documents\tabele\slownik_zw.xlsx"
        template = pd.read_excel(src_slownik, sheet_name='summary_template', header=None, index_col=0)
        wkb = load_workbook(self.lok)
        wkb.create_sheet('Reach Estimation')
        wkb.save(self.lok)




    def __init__(self, parent):
        self.path = None
        self.main_tab = None
        self.vectors = None
        self.engine = None
        self.target_list = None
        self.selected_target = None
        self.ref_tg = None
        self.endo_vector = None
        self.exo_vectors = None
        self.new_tab = None
        self.reach1 = None
        self.reach3 = None
        self.window_app = tk.Frame(parent, height=600, width=600*1.618)
        self.window_app.winfo_toplevel().title("RAGE: Reach And GRP Estimator")
        self.window_app.pack()
        self.import_button = tk.Button(parent, text="Upload Your Campaign", command=self.file_import,
                                       activeforeground="purple1", activebackground="pale green",
                                       highlightcolor="pale green", bg='azure2')
        self.import_button.place(x=30, y=30, height=30, width = 200)
        self.process_button = tk.Button(parent, text="Estimate Client's Target GRPs", command=self.file_export, bg='azure2',
                                       activeforeground="purple1", activebackground="pale green")
        self.process_button.place(x=30, y=70, height=30, width=200)

        self.process_button = tk.Button(parent, text="Estimate Campaign's Reach", bg='azure2', command=self.estimate_reach)
        self.process_button.place(x=30, y=110, height=30, width=200)
        self.update_button = tk.Button(parent, text="Update Campaigns Database", command=self.update_db, bg='sandy brown')
        self.update_button.place(x=30, y=190, height=30, width = 200)


root = tk.Tk()
app = MainApp(root)
root.mainloop()

tab = app.new_tab
# ref_tg = app.selected_target

# app.path
# app.main_tab
#
# print(type(app.path))