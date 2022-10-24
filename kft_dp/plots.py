import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

def plot_line(df:pd.DataFrame, x_col:str, y_col:str, title:= None, hue= None,  linewidth =2):
    plt.figure(figsize=(15,8))
    sns.lineplot(data = df, y = y_col, x= x_col,  palette= 'pastel')
    plt.show()


def plot_hist(df: pd.DataFrame, column: str) -> None:
    plt.figure(figsize=(15, 8))
    sns.displot(data=df, x=column, color=color, kde=True, height=7, aspect=2, palette= 'pastel')
    plt.title(f'Distribution of {column}', size=20)
    plt.show()


def plot_count(df: pd.DataFrame, column: str, title= None) -> None:
    plt.figure(figsize=(15, 8))
    sns.countplot(data=df, x=column, palette= 'pastel')
    plt.title(title, size=20)
    plt.show()


def plot_bar(df: pd.DataFrame, x_col: str, y_col: str, title = None, xlabel= None, ylabel = None) -> None:
    plt.figure(figsize=(15, 8))
    sns.barplot(data=df, x=x_col, y=y_col, palette= 'pastel')
    plt.title(title, size=20)
    plt.xticks(rotation=75, fontsize=14)
    plt.yticks(fontsize=14)
    plt.xlabel(xlabel, fontsize=16)
    plt.ylabel(ylabel, fontsize=16)
    plt.show()


def plot_heatmap(correlation, title: str):
    plt.figure(figsize=(15, 8))
    sns.heatmap(correlation)
    plt.title(title, size=18, fontweight='bold')
    plt.show()


def plot_box(df: pd.DataFrame, x_col: str, title = None) -> None:
    plt.figure(figsize=(15, 8))
    sns.boxplot(data=df, x=x_col, palette= 'pastel')
    plt.title(title, size=20)
    plt.xticks(rotation=75, fontsize=14)
    plt.show()


def plot_scatter(df: pd.DataFrame, x_col: str, y_col: str, title: str, hue: None, style: None) -> None:
    plt.figure(figsize=(15, 8))
    sns.scatterplot(data=df, x=x_col, y=y_col, hue=hue, style=style, palette= 'pastel')
    plt.title(title, size=20)
    plt.xticks(fontsize=14)
    plt.yticks(fontsize=14)
    plt.show()

# def plot_save(plot, filename:str):
#     fig.savefig(filename, dpi=250)



