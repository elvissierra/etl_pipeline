import pandas as pd
import numpy as np
import statsmodels.api as sm


def run_popularity_logistic(
    df: pd.DataFrame,
    popular_col: str = "Popularity",
    flag_col: str = "Is POI also a tourist attraction?",
):
    """
    Fits a logistic regression to predict a binary flag from a continuous popularity score.

    Args:
        df: DataFrame containing the data.
        popular_col: Name of the numeric popularity column.
        flag_col: Name of the binary flag column (values 'Yes'/'No').

    Returns:
        model: The fitted Logit model.
        odds_ratios: Series of odds ratios for each parameter.
        prob_df: DataFrame with a grid of popularity values and predicted probabilities.
    """
    # Prepare data
    # 1) Drop rows missing Popularity or the raw flag
    df_lr = df.dropna(subset=[popular_col, flag_col]).copy()

    # 2) Map only 'Yes'/'No' to 1/0, leave everything else as NaN
    df_lr["tourist_flag"] = df_lr[flag_col].map({"Yes": 1, "No": 0})

    # 3) Drop any rows where tourist_flag is NaN (i.e. filter out 'Unverifiable', etc.)
    df_lr = df_lr.dropna(subset=["tourist_flag"])
    # df_lr['tourist_flag'] = df_lr['tourist_flag'].astype(int)

    # # Design matrix with constant
    # X = sm.add_constant(df_lr[popular_col])

    df_lr["high_pop_500"] = df_lr["Popularity"] >= 500
    X = sm.add_constant(df_lr["high_pop_500"].astype(int))
    y = df_lr["tourist_flag"]

    # Fit logistic regression
    model = sm.Logit(y, X).fit(disp=False)

    # Compute odds ratios
    odds_ratios = np.exp(model.params)

    # Generate prediction grid
    pop_min, pop_max = df_lr[popular_col].min(), df_lr[popular_col].max()
    grid = np.linspace(pop_min, pop_max, 10)
    prob_df = pd.DataFrame({popular_col: grid})
    prob_df = sm.add_constant(prob_df)
    prob_df["Predicted_Prob"] = model.predict(prob_df)

    return model, odds_ratios, prob_df
