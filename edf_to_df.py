from datetime import datetime
import pandas as pd
from typing import Any, Callable, Dict, Hashable, List, Tuple, Union


def edf_to_df(file: str) -> Tuple[pd.DataFrame, Dict[Hashable, Any]]:

    df = pd.DataFrame()

    raw: bytes
    with open(file, 'rb') as f:
        raw = f.read()

    # HEADER RECORD
    # Apply to metadata attrs of DataFrame
    df.attrs['version'] = int(raw[0:8])
    df.attrs['patient_id'] = raw[8:88].decode().rstrip(' ')
    df.attrs['recording_id'] = raw[88:168].decode().rstrip(' ')
    df.attrs['start'] = datetime.strptime(
        raw[168:184].decode().rstrip(' '),
        "%d.%m.%y%H.%M.%S"
    )
    df.attrs['header_bytes'] = int(raw[184:192])
    df.attrs['n_data_records'] = int(raw[236:244])
    df.attrs['s_data_records'] = int(raw[244:252])
    df.attrs['n_signals'] = int(raw[252:256])

    def signals_info_parse(b: bytes, st: int, div: int, ns: int, c: Callable) -> Tuple[List[Any], int]:

        en: int = st + div
        result: List[Any] = []

        for nr in range(0, ns):
            r: Any = c(b[st:en])
            result.append(r)
            st += div
            en += div
        en -= div
        return result, en

    def int_or_string(b: bytes) -> Union[int, str]:
        result: Union[int, str]
        try:
            result = int(b)
        except ValueError:
            result = b.decode().rstrip(' ')
        return result

    # Set starting int
    start: int = 256
    # Grab amount of signals
    n_signals: int = df.attrs['n_signals']
    # Define byte lengths
    divs: List[int] = [16, 80, 8, 8, 8, 8, 8, 80, 8]
    # Define dict keys for df attrs
    keys: List[str] = [
        'signal_labels',
        'signal_transducers',
        'signal_dimensions',
        'signal_physical_minimums',
        'signal_physical_maximums',
        'signal_digital_minimums',
        'signal_digital_maximums',
        'signal_prefilters',
        'signal_n_samples'
    ]

    # Loop for signal information
    for k, d in zip(keys, divs):
        df.attrs[k], start = signals_info_parse(
            raw,
            start,
            d,
            n_signals,
            int_or_string
        )

    # Reserved space
    start += n_signals * 32

    # TODO: Annotations parsing

    # Redefine raw to remove unnecessary bytes
    raw = raw[start:]

    backup = df.attrs

    for i, n in enumerate(backup['signal_n_samples']):
        w_start: int = sum(backup['signal_n_samples'][:i]) * 2
        w_after: int = sum(backup['signal_n_samples'][i:]) * 2
        w_end: int = w_start + (n * 2)
        step: int = w_start + w_after

        data = b''
        for dr in range(0, backup['n_data_records']):
            data = data + raw[w_start:w_end]
            w_start += step
            w_end += step

        int_form = [int.from_bytes(data[d:d+2], byteorder='little', signed=True) for d in range(0, len(data), 2)]

        p: int = (backup['n_data_records'] * n)

        date_range = pd.date_range(
            backup['start'],
            periods=p,
            freq=f'{backup["s_data_records"]}ms'
        )

        s = pd.Series(int_form, index=date_range)

        temp_df = pd.DataFrame(
            {f"{backup['signal_labels'][i]}": s}
        )

        df = df.join(temp_df, how="outer")

    df.attrs = backup

    return df, backup


# if __name__ == "__main__":
#
#     df, b = edf_to_df("ST7011J0-PSG.edf")
#     print(b['signal_physical_minimums'])
#     print(b['signal_physical_maximums'])
#     print(b['signal_digital_minimums'])
#     print(b['signal_digital_maximums'])
#     print("EEG Fpz-Cz: ")
#     print(df['EEG Fpz-Cz'].unique())
#     print(df['EEG Fpz-Cz'].max())
#     print(df['EEG Fpz-Cz'].min())
#     print("EEG Pz-Oz: ")
#     print(df['EEG Pz-Oz'].unique())
#     print(df['EEG Pz-Oz'].max())
#     print(df['EEG Pz-Oz'].min())
#     print("Marker: ")
#     print(df['Marker'].unique())
#     print(df['Marker'].max())
#     print(df['Marker'].min())
