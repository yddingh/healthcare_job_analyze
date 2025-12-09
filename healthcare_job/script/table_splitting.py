# split_and_save_compact.py
import os
import pandas as pd
from sqlalchemy import create_engine

INPUT_CSV = r"C:\Users\JKK4V3PX\healthcare_job_analyze\healthcare_job\data\cleaned_healthcare_jobs.csv"
SQLITE_DB = "healthcare.db"
OUTDIR = "output_split_simple"
os.makedirs(OUTDIR, exist_ok=True)

def read_csv_safe(path):
    try:
        df = pd.read_csv(path, parse_dates=['post_date_parsed'])
    except Exception:
        df = pd.read_csv(path)
    # strip column names
    df.columns = [c.strip() for c in df.columns]
    return df

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def build_dim(df, col_names, id_name, keep_cols=None):
    """通用去重生成维表。col_names: list of cols to base distinct on.
       keep_cols: output cols order besides id_name (if None use col_names)."""
    keep_cols = keep_cols or col_names
    tmp = df[col_names].copy().drop_duplicates().reset_index(drop=True)
    tmp = tmp.reset_index().rename(columns={'index': id_name})
    tmp[id_name] = tmp[id_name] + 1
    return tmp[[id_name] + keep_cols]

def main():
    df = read_csv_safe(INPUT_CSV)
    print("Loaded rows:", len(df), "cols:", df.columns.tolist())

    # 需要的基础列（若不存在就补 NA）
    base_cols = [
        'job_id','post_date_parsed','category','job_type_std','job_title',
        'job_description_clean','desc_word_count','skill_flags','company_name',
        'city','country','salary_offered','salary_min','salary_max','salary_currency','salary_parsed'
    ]
    df = ensure_cols(df, base_cols)

    # 生成维表
    dim_category = build_dim(df, ['category'], 'category_id')
    dim_job_type = build_dim(df, ['job_type_std'], 'job_type_id')
    dim_company = build_dim(df, ['company_name','city','country'], 'company_id', keep_cols=['company_name','city','country'])
    dim_salary = build_dim(df, ['salary_offered','salary_min','salary_max','salary_currency','salary_parsed'], 'salary_id')

    # 构建事实表（保留关键列）
    fact = df[[
        'job_id','post_date_parsed','category','job_type_std','job_title',
        'job_description_clean','desc_word_count','skill_flags','company_name','city','country',
        'salary_offered','salary_min','salary_max','salary_currency','salary_parsed'
    ]].copy()

    # 合并外键（最小化步骤）
    fact = fact.merge(dim_category, how='left', on='category')
    fact = fact.merge(dim_job_type, how='left', on='job_type_std')

    # company: 精确合并 company_name+city+country，若缺失再回退 company_name
    fact = fact.merge(dim_company, how='left', on=['company_name','city','country'])
    if fact['company_id'].isna().any():
        fallback = dim_company.drop_duplicates(subset=['company_name'])[['company_name','company_id']].rename(columns={'company_id':'company_id_fb'})
        fact = fact.merge(fallback, how='left', on='company_name')
        fact['company_id'] = fact['company_id'].fillna(fact['company_id_fb'])
        fact.drop(columns=['company_id_fb'], inplace=True)

    # salary: 精确合并；若按全部列匹配失败则按 salary_offered 回退
    sal_cols = ['salary_offered','salary_min','salary_max','salary_currency','salary_parsed']
    fact = fact.merge(dim_salary, how='left', on=sal_cols)
    if fact['salary_id'].isna().any():
        fb = dim_salary.dropna(subset=['salary_offered']).drop_duplicates(subset=['salary_offered'])[['salary_offered','salary_id']].rename(columns={'salary_id':'salary_id_fb'})
        fact = fact.merge(fb, how='left', on='salary_offered')
        fact['salary_id'] = fact['salary_id'].fillna(fact['salary_id_fb'])
        fact.drop(columns=['salary_id_fb'], inplace=True)

    # 取最终事实表列
    fact_jobs = fact[[
        'job_id','post_date_parsed','category_id','job_type_id',
        'job_title','job_description_clean','desc_word_count','skill_flags',
        'company_id','salary_id'
    ]]

    # 导出 CSV
    dim_category.to_csv(os.path.join(OUTDIR, 'dim_category.csv'), index=False)
    dim_job_type.to_csv(os.path.join(OUTDIR, 'dim_job_type.csv'), index=False)
    dim_company.to_csv(os.path.join(OUTDIR, 'dim_company.csv'), index=False)
    dim_salary.to_csv(os.path.join(OUTDIR, 'dim_salary.csv'), index=False)
    fact_jobs.to_csv(os.path.join(OUTDIR, 'fact_jobs.csv'), index=False)
    print("Saved CSVs to", OUTDIR)

    # 写入 sqlite
    engine = create_engine(f"sqlite:///{SQLITE_DB}")
    dim_category.to_sql('dim_category', engine, if_exists='replace', index=False)
    dim_job_type.to_sql('dim_job_type', engine, if_exists='replace', index=False)
    dim_company.to_sql('dim_company', engine, if_exists='replace', index=False)
    dim_salary.to_sql('dim_salary', engine, if_exists='replace', index=False)
    fact_jobs.to_sql('fact_jobs', engine, if_exists='replace', index=False)
    print("Wrote tables to", SQLITE_DB)

    # 导出未匹配样例（方便检查）
    if fact_jobs[['category_id','company_id','salary_id']].isna().any().any():
        fact_jobs[['category_id','company_id','salary_id']].isna().sum().to_frame('missing_count').to_csv(os.path.join(OUTDIR,'missing_fk_counts.csv'))
        print("Some foreign keys missing — saved missing_fk_counts.csv for inspection.")

if __name__ == "__main__":
    main()
