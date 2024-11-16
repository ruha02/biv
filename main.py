import csv
import re
import warnings

import pandas as pd
from pandas import DataFrame

warnings.filterwarnings("ignore")


def replace_newlines(input_file_path) -> str:
    """
    Заменяет в файле все вхождения \n[^0-9] на пустую строку
    """
    with open(input_file_path, "r", encoding="utf-8") as file:
        content = file.read()
    modified_content = ""
    while modified_content != content:
        modified_content = content
        content = re.sub(r"\n[^0-9]", "", content)
    with open(input_file_path, "w", encoding="utf-8") as file:
        file.write(modified_content)
    return modified_content


def clean_duplicates(df) -> DataFrame:
    df_cleaned = df.copy()
    duplicates_mask = (
        df_cleaned.duplicated(subset=["inn", "company_id", "share"], keep="first")
        & df_cleaned["share_percent"].isna()
    )
    """
        Очищаем датасет от дубликатов
        Дубликатом считается если одинаковые inn, company_id, share и share_percent пустой.
        Если будут найдены записи у которых одинаковые inn, company_id, share, но при этом будет существовать у одной записи shared_percent, то другие удалить
        Если компании-учредителя нет в датасете компаний, то удаляем, поскольку у нас нет company_id данной компании, чтобы найти ее учредителей
    """
    df_cleaned = df_cleaned[~duplicates_mask]

    df_cleaned = (
        df_cleaned.groupby(["inn", "company_id", "share"])
        .apply(
            lambda x: (
                x[x["share_percent"].notna()]
                if (len(x) > 1 and x["share_percent"].notna().any())
                else x
            )
        )
        .reset_index(drop=True)
    )
    return df_cleaned


def get_percent_na_share_percent() -> float:
    return founders["share_percent"].isna().sum() / len(founders)


def fill_missing_share_percent_by_share(df) -> DataFrame:
    """
    Если для одного company_id есть все значения share, то share_percent расчитывается как доля share к общей доле.
    """

    def action(group) -> DataFrame:
        mask = group.index
        if group["share_percent"].isna().any():
            share_sum = group["share"].sum()
            df.loc[mask, "share_percent"] = group["share"] / share_sum

    return df.groupby("company_id", group_keys=False).apply(action)


def fill_missing_share_percent_to_single_founders(df) -> DataFrame:
    """
    Если для одного company_id в share_percent отсутсвует одно значение, то оно вычисляется как разница 1 к сумме оставшихся значений.
    """

    def action(group) -> DataFrame:
        if len(group) == 1:
            group["share_percent"] = 1.0
            return group

    return df.groupby("company_id", group_keys=False).apply(action)


def fill_missing_share_percent_by_other_share_percent(df) -> DataFrame:
    """
    Если для одного company_id share_percent и share отсутвуют и существует только один учредитель, то значение share_percent равно 1.0
    """

    def action(group) -> DataFrame:
        missing_count = group["share_percent"].isna().sum()
        if missing_count == 1:
            existing_sum = group["share_percent"].sum()
            missing_idx = group[group["share_percent"].isna()].index[0]
            df.loc[missing_idx, "share_percent"] = 1.0 - existing_sum

    return df.groupby("company_id", group_keys=False).apply(action)


def fill_missing_share_percent_by_ratio(df) -> DataFrame:
    """
    Если для одного company_id есть учредитель у которого есть пара share и share_percent, то использовать соотношение этих значений, чтобы найти неизвестное значение share_percent для других учредителей
    """

    def action(group) -> DataFrame:
        reference = group[
            (group["share"].notna())
            & (group["share_percent"].notna())
            & (group["share"] > 0)
            & (group["share_percent"] > 0)
        ]
        if not reference.empty:
            ref_row = reference.iloc[0]
            ratio = ref_row["share_percent"] / ref_row["share"]
            missing_mask = group["share_percent"].isna() & group["share"].notna()
            group.loc[missing_mask, "share_percent"] = (
                group.loc[missing_mask, "share"] * ratio
            )
            return group

    return df.groupby("company_id", group_keys=False).apply(action)


def calculate_ownership(founders_df, df_company) -> DataFrame:
    """
    Составление списков учредителей с учетом косвенных долей
    """
    result = []

    def get_ownership_chain(company_id, current_share=1.0, visited=None):
        if visited is None:
            visited = set()

        if company_id in visited:
            return

        visited.add(company_id)

        company_founders = founders_df[founders_df["company_id"] == company_id]

        for _, founder in company_founders.iterrows():
            new_share = current_share * founder["share_percent"]

            if founder["is_person"] == 1:
                result.append(
                    {
                        "company_id": company_id,
                        "inn": founder["inn"],
                        "share_percent": new_share,
                    }
                )
            else:
                # Если владелец - компания, рекурсивно вызываем функцию для этой компании
                founder_company = df_company[df_company["inn"] == founder["inn"]]
                if not founder_company.empty:
                    get_ownership_chain(
                        founder_company.iloc[0]["id"], new_share, visited.copy()
                    )

    for company_id in df_company["id"].unique():
        get_ownership_chain(company_id)

    return pd.DataFrame(result)


def create_tsv_output(df_company, n_founder, ownership, output_file="results.tsv"):
    """
    Формируем результирующий файл в формате TSV
    """
    valid_companies = ownership[ownership["share_percent"] >= 0.25][
        "company_id"
    ].unique()
    valid_companies.sort()
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        for company_id in valid_companies:
            company = df_company[df_company["id"] == company_id]
            if len(company) == 0:
                continue
            else:
                company = company.iloc[0]
            company_founders = ownership[
                (ownership["company_id"] == company_id)
                & (ownership["share_percent"] >= 0.25)
            ]
            if len(company_founders) > 0:
                writer.writerow(
                    [company_id, company["ogrn"], company["inn"], company["full_name"]]
                )
                for _, founder in company_founders.iterrows():
                    person = n_founder[n_founder["inn"] == founder["inn"]]
                    if len(person) == 0:
                        continue
                    else:
                        person = person.iloc[0]
                    full_name = f"{person['last_name']} {person['first_name']} {person['second_name']}"
                    writer.writerow(
                        [
                            "",
                            f"{str(int(founder['inn'])):012}",
                            full_name,
                            f"{founder['share_percent'] * 100:.2f}",
                        ]
                    )


if __name__ == "__main__":
    rows = replace_newlines("company.tsv")
    print("🟢 Проверка файла company.tsv завершена.")
    rows = replace_newlines("founder_legal.tsv")
    print("🟢 Проверка файла founder_legal.tsv завершена.")
    rows = replace_newlines("founder_natural.tsv")
    print("🟢 Проверка файла founder_natural.tsv завершена.")
    df_company = pd.read_csv("company.tsv", sep="\t")
    l_founder = pd.read_csv("founder_legal.tsv", sep="\t")
    n_founder = pd.read_csv("founder_natural.tsv", sep="\t")
    founders = pd.concat(
        [
            l_founder[["inn", "company_id", "share", "share_percent"]].assign(
                is_person=0
            ),
            n_founder[["inn", "company_id", "share", "share_percent"]].assign(
                is_person=1
            ),
        ]
    )
    print(f"Количество учредителей до очистки ФЛ:  {len(founders)}")
    founders = clean_duplicates(founders)
    print(f"Количество учредителей после очистки ФЛ:  {len(founders)}")
    print(f"Количество учредителей до очистки ЮЛ:  {len(founders)}")
    founders = founders[
        (founders["is_person"] == 1)
        | (
            (founders["is_person"] == 0)
            & (founders["inn"].isin(df_company["inn"].unique()))
        )
    ]
    print(f"Количество учредителей после очистки ЮЛ:  {len(founders)}")
    print("🟢 Известны все доли в рублях у всех урчедителей")
    print(
        f"Процент незаполненных share_percent: {get_percent_na_share_percent()*100:.2f}%"
    )
    fill_missing_share_percent_by_share(founders)
    print(
        f"Процент незаполненных share_percent (после предобработки): {get_percent_na_share_percent()*100:.2f}%"
    )

    print("🟢 Единный учредитель")
    print(
        f"Процент незаполненных share_percent: {get_percent_na_share_percent()*100:.2f}%"
    )
    fill_missing_share_percent_to_single_founders(founders)
    print(
        f"Процент незаполненных share_percent (после предобработки): {get_percent_na_share_percent()*100:.2f}%"
    )

    print("🟢 Известны все доли в процентах, кроме одной")
    print(
        f"Процент незаполненных share_percent: {get_percent_na_share_percent()*100:.2f}%"
    )
    fill_missing_share_percent_by_other_share_percent(founders)
    print(
        f"Процент незаполненных share_percent (после предобработки): {get_percent_na_share_percent()*100:.2f}%"
    )

    print("🟢 Известна пропорция одного из участников")
    print(
        f"Процент незаполненных share_percent: {get_percent_na_share_percent()*100:.2f}%"
    )
    fill_missing_share_percent_by_ratio(founders)
    print(
        f"Процент незаполненных share_percent (после предобработки): {get_percent_na_share_percent()*100:.2f}%"
    )
    print("🟢 Составление списков учредителей с учетом косвенных долей")
    ownership = calculate_ownership(founders, df_company)
    print("🟢 Формирование выходного файла")
    create_tsv_output(df_company, n_founder, founders)
