# Importaciones de librerías necesarias
import os
import requests
import json
import datetime as dt
import pandas as pd

'''Script para importar datos de ofertas de trabajo desde la página getonbrd.cl

La página cuenta con una API para hacer requests de información, por lo que se usa el módulo requests en conjunto a
json para importar los datos de ofertas de trabajo del último día, extraer y ordenar la información útil en un dataframe
de Pandas, y por último exportar este dataframe a un archivo .csv'''

# Se setea el directorio donde se trabaja
workingpath = '/home/kjal/jobs_csv'
if os.path.isdir(workingpath):
    os.chdir(workingpath)
else:
    os.mkdir(workingpath)
    os.chdir(workingpath)

# Se hace un request a la API de getonbrd.cl
response = requests.get("https://www.getonbrd.com/api/v0/search/jobs?query='chile'&per_page=100&page=1")

# Se inicializan variables
today = dt.datetime.today().strftime('%Y-%m-%d')
jobs = []
perks = []
techs = []
chile_list = ["chile", "santiago", "valparaiso", "iquique", "antofagasta", "concepción", "montt"]
pattern = '|'.join(chile_list)
clouds = ["Amazon Web Services", "AWS", "Google Cloud Platform", "Google Cloud", "GCP", "Azure"]
lang = ["Python", "Java ", "JavaScript", "Kotlin", "PHP", "Ruby", "Scala", " R ", "C++", "C#", "Swift", "SQL"]
jobs_fields = ["jobid", "title", "remote", "remote_modality", "remote_zone", "country", "lang",
               "category", "min_salary", "max_salary", "application_count", "modality", "seniority",
               "company", "pub_date"]
perk_fields = ["perk_1", "perk_2", "perk_3", "perk_4", "perk_5", "perk_6", "perk_7", "perk_8",
               "perk_9", "perk_10", "perk_11", "perk_12", "perk_13", "perk_14", "perk_15",
               "perk_16", "perk_17", "perk_18", "perk_19", "perk_20"]
tech_fields = ["AWS", "GCP", "Azure", "Python", "Java", "JavaScript", "Kotlin", "PHP", "Ruby",
               "Scala", " R ", "C++", "C#", "Swift", "SQL"]

# Ciclo principal, por cada objeto de la respuesta JSON (una oferta de trabajo) realiza una verificación de los atributos de interés
# y los agrega a las listas correspondientes según el área ('jobs' para características generales de la oferta, 'perks' para los 
# beneficios ofrecidos y 'techs' para las tecnologías mencionadas en los requerimientos)
for job in response.json()["data"]:
    att = job['attributes']
    pub_date = dt.datetime.utcfromtimestamp(job['attributes']['published_at']).strftime("%Y-%m-%d")
    zone_aux = att["remote_zone"] or ""
    in_chile = att["title"] + zone_aux.lower() + att["country"].lower()

    contains_chile = False
    for word in chile_list:
        if word in in_chile:
            contains_chile = True
            break

    if pub_date == today and contains_chile:
        tempjobs = []
        tempperks = []
        temptechs = []

        tempjobs.extend(
            [job['id'], att['title'], att['remote'], att['remote_modality'], att['remote_zone'], att['country'],
             att['lang'], att['category_name'], att['min_salary'], att['max_salary'], att['applications_count'],
             att['modality']['data']['id'], att['seniority']['data']['id'], att['company']['data']['id']])
        tempjobs.append(pub_date)

        job_text = att["description"] + att["desirable"] + att["functions"] + att["projects"]

        AWS, GCP, MS = None, None, None
        if clouds[0] in job_text or clouds[1] in job_text:
            AWS = int(1)
        if clouds[2] in job_text or clouds[3] in job_text or clouds[4] in job_text:
            GCP = int(1)
        if clouds[5] in job_text:
            MS = int(1)
        temptechs.extend([AWS, GCP, MS])

        Python, Java, JavaScript, Kotlin, PHP, Ruby, Scala, R, Cplus, Ccat, Swift, SQL = None, None, None, None, None, \
                                                                                         None, None, None, None, None, \
                                                                                         None, None
        Python = 'Python' if lang[0].lower() in job_text.lower() else 'Nulo'
        Java = 'Java' if (lang[1].lower() in job_text.lower() or "java," in job_text.lower()) else 'Nulo'
        JavaScript = 'JS' if (lang[2].lower() in job_text.lower() or ".js" in job_text.lower()) else 'Nulo'
        Kotlin = 'Kotlin' if lang[3].lower() in job_text.lower() else 'Nulo'
        PHP = 'PHP' if lang[4].lower() in job_text.lower() else 'Nulo'
        Ruby = 'Ruby' if lang[5].lower() in job_text.lower() else 'Nulo'
        Scala = 'Scala' if lang[6].lower() in job_text.lower() else 'Nulo'
        R = 'R' if (lang[7].lower() in job_text.lower() or " R," in job_text) else 'Nulo'
        Cplus = 'C++' if lang[8].lower() in job_text.lower() else 'Nulo'
        Ccat = 'C#' if lang[9].lower() in job_text.lower() else 'Nulo'
        Swift = 'Swift' if lang[10].lower() in job_text.lower() else 'Nulo'
        SQL = 'SQL' if lang[11].lower() in job_text.lower() else 'Nulo'
        temptechs.extend([Python, Java, JavaScript, Kotlin, PHP, Ruby, Scala, R, Cplus, Ccat, Swift, SQL])

        jobs.append(tempjobs)
        tempperks.extend(att['perks'][0:20])
        for i in range(20 - len(tempperks)):
            tempperks.append(None)

        perks.append(tempperks)
        techs.append(temptechs)

# Se crean los dataframes a partir de los datos de cada lista de listas
jobs_df = pd.DataFrame(jobs, columns=jobs_fields)
perks_df = pd.DataFrame(perks, columns=perk_fields)
techs_df = pd.DataFrame(techs, columns=tech_fields)

# Adicionalmente se reemplazan los valores internos usados por getonbrd.cl por denominaciones comprensibles
jobs_df["modality"] = jobs_df["modality"].astype(str).replace(["1", "2", "3", "4"],
                                                              ["Full Time", "Part time", "Freelance",
                                                               "Práctica/Internship"])
jobs_df["seniority"] = jobs_df["seniority"].astype(str).replace(["1", "2", "3", "4", "5"],
                                                                ["No exp", "Junior", "Semi Senior", "Senior",
                                                                 "Experto"])

# Debido a que es una base de datos relativamente pequeña se decide tener todas las columnas en una sola tabla
# denormalizada, además de ser una configuración óptima para BigQuery
jobs_df = pd.concat([jobs_df, techs_df, perks_df], axis=1)

# La tabla final se exporta a un .csv con el nombre <YYYY_mm_dd>_jobs.csv
jobs_df.to_csv(today + "_jobs.csv", index=False)
