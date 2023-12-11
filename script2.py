import os
import time
import glob
import pandas as pd
from datetime import timedelta, datetime


STOP_SIGNAL_FILE = 'stop_signal.txt'  # File to signal script1.py to stop

current_datetime = datetime.now()
seconds = current_datetime.strftime("%S")
minutes = current_datetime.strftime("%M")
date = current_datetime.strftime("%d%m%Y")
today = seconds+minutes+date


def save_df_csv(df, filename):
    if not os.path.exists('result'):
        os.makedirs('result')
    df.to_csv(f'result/{filename}.csv')


def signal_stop():
    open(STOP_SIGNAL_FILE, 'w').close()


def convert_format(date_str):
    try:
        return pd.to_datetime(date_str, format='%d%m%Y %I:%M %p').strftime('%m/%d/%Y %H:%M:%S')
    except ValueError:
        return date_str


def read_files(directory):
    result_df = pd.DataFrame()
    for file_name in glob.glob(directory + '*.csv'):
        x = pd.read_csv(file_name, low_memory=False)
        result_df = pd.concat([result_df, x], axis=0)
    return result_df


def analyze_bets(bets_df):
    bets_df["coef"] = bets_df['payout']/bets_df['amount']
    # Finding consecutive wins with coef > 1.5
    bets_df['consecutive_wins'] = (bets_df['result'] == 'Win') & (bets_df['coef'] > 1.5)
    bets_df['group'] = (bets_df['consecutive_wins'] != bets_df['consecutive_wins'].shift(1)).cumsum()
    consecutive_wins_df = bets_df[bets_df['consecutive_wins']]

    # Finding users with 5 consecutive wins
    users_with_5_consecutive_wins = consecutive_wins_df.groupby(['player_id', 'group']).filter(lambda x: len(x) >= 5)

    return users_with_5_consecutive_wins.drop_duplicates(subset=['player_id'])


def analyze_deposit_behavior(bet_df, payments_df):
    deposit_df = payments_df[(payments_df['transaction_type'] == 'deposit') &
                             (payments_df['status'] == 'Approved')]
    withdrawal_df = payments_df[payments_df['transaction_type'] == 'withdrawal']

    withdrawal_df = withdrawal_df[['player_id', 'Date', 'payment_method_name']]

    cartesian_df = pd.merge(deposit_df, withdrawal_df, on='player_id', how='outer')

    # Convert date
    bet_df['accept_time'] = bet_df['accept_time'].apply(convert_format)
    bet_df['accept_time'] = pd.to_datetime(bet_df['accept_time'], format='mixed')

    cartesian_df['Date_x'] = pd.to_datetime(cartesian_df['Date_x'], format='mixed', errors='coerce')
    cartesian_df['Date_y'] = pd.to_datetime(cartesian_df['Date_y'], format='mixed', errors='coerce')
    cartesian_df['paid_amount'] = pd.to_numeric(cartesian_df['paid_amount'], errors='coerce')

    cartesian_df = cartesian_df.loc[
        (cartesian_df['Date_y'] - cartesian_df['Date_x'] <= timedelta(hours=1)) &
        (cartesian_df['Date_x'] < cartesian_df['Date_y']) &
        (cartesian_df['payment_method_name_x'] != cartesian_df['payment_method_name_y'])]
    bet_df['player_id'] = bet_df['player_id'].astype(str)
    cartesian_df = pd.merge(cartesian_df, bet_df, on='player_id', how='inner')

    cartesian_df = cartesian_df.loc[
        (cartesian_df['amount'] >= 0.9 * cartesian_df['paid_amount']) &
        (cartesian_df['amount'] <= 1.1 * cartesian_df['paid_amount']) &
        (cartesian_df['accept_time'] >= cartesian_df['Date_x']) &
        (cartesian_df['accept_time'] <= cartesian_df['Date_y'])]
    return cartesian_df.drop_duplicates(subset=['player_id'])


def main():
    bet_df = read_files("bets/")
    bet_df.set_index('bet_id', inplace=True)

    payments_df = read_files("payments/")
    payments_df.set_index('id', inplace=True)

    final_df_behabior = analyze_deposit_behavior(bet_df, payments_df)
    final_df_bets = analyze_bets(bet_df)
    save_df_csv(final_df_behabior, f'result{today}')
    save_df_csv(final_df_bets, f'bets{today}')
    pass


if __name__ == "__main__":
    main()
    signal_stop()
