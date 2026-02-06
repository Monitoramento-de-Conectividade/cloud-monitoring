import tkinter as tk
from tkinter import messagebox
import os
import subprocess
import sys
import webbrowser

from cloudv2_config import get_config_file_path, load_editable_config, normalize_config, save_config


BG = "#EAF7EC"
PANEL = "#D5EDD8"
ACCENT = "#2E7D32"
ACCENT_DARK = "#1B5E20"
TEXT = "#1D3B29"
INPUT_BG = "#FFFFFF"


class ConfigUI:
    def __init__(self, root):
        self.root = root
        self.config_path = get_config_file_path()
        self.root.title("CloudV2 - Configurador MQTT")
        self.root.configure(bg=BG)
        self.root.geometry("1320x760")
        self.root.minsize(1180, 680)

        self.broker_var = tk.StringVar()
        self.port_var = tk.StringVar()
        self.info_topic_var = tk.StringVar()
        self.min_minutes_var = tk.StringVar()
        self.max_minutes_var = tk.StringVar()
        self.timeout_var = tk.StringVar()
        self.ping_interval_var = tk.StringVar()
        self.ping_topic_var = tk.StringVar()
        self.dashboard_port_var = tk.StringVar()
        self.history_mode_var = tk.StringVar(value="merge")
        self.cmd_topic_input_var = tk.StringVar()
        self.topic_input_var = tk.StringVar()
        self.filter_input_var = tk.StringVar()
        self.status_var = tk.StringVar(value=f"Arquivo de configuracao: {self.config_path}")

        self._build_ui()
        self._load_data()

    def _build_ui(self):
        header = tk.Frame(self.root, bg=ACCENT, padx=16, pady=14)
        header.pack(fill="x")
        tk.Label(
            header,
            text="CloudV2 - Editor de Monitoramento e Comandos",
            bg=ACCENT,
            fg="white",
            font=("Segoe UI", 16, "bold"),
        ).pack(anchor="w")
        tk.Label(
            header,
            text="Configure comandos #11$, monitoramento preciso e monitoramento leve sem editar codigo.",
            bg=ACCENT,
            fg="#E8F5E9",
            font=("Segoe UI", 10),
        ).pack(anchor="w")

        body = tk.Frame(self.root, bg=BG, padx=16, pady=16)
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=1)
        body.rowconfigure(1, weight=1)

        settings = tk.Frame(body, bg=PANEL, padx=14, pady=12, bd=1, relief="solid")
        settings.grid(row=0, column=0, sticky="ew")
        self._build_settings(settings)

        lists = tk.Frame(body, bg=BG)
        lists.grid(row=1, column=0, sticky="nsew", pady=(14, 0))
        lists.columnconfigure(0, weight=1)
        lists.columnconfigure(1, weight=1)
        lists.columnconfigure(2, weight=1)
        lists.rowconfigure(0, weight=1)

        self.cmd_topics_listbox = self._build_list_panel(
            lists,
            col=0,
            title="Monitoramento mais preciso",
            placeholder="Topicos de comando que vao receber #11$",
            input_var=self.cmd_topic_input_var,
            add_command=lambda: self._add_to_list(self.cmd_topics_listbox, self.cmd_topic_input_var),
            remove_command=lambda: self._remove_selected(self.cmd_topics_listbox),
            up_command=lambda: self._move_selected(self.cmd_topics_listbox, -1),
            down_command=lambda: self._move_selected(self.cmd_topics_listbox, 1),
        )

        self.filters_listbox = self._build_list_panel(
            lists,
            col=1,
            title="Monitoramento leve (nomes)",
            placeholder="Nomes/pivots para filtrar no payload",
            input_var=self.filter_input_var,
            add_command=lambda: self._add_to_list(self.filters_listbox, self.filter_input_var),
            remove_command=lambda: self._remove_selected(self.filters_listbox),
            up_command=lambda: self._move_selected(self.filters_listbox, -1),
            down_command=lambda: self._move_selected(self.filters_listbox, 1),
        )

        self.topics_listbox = self._build_list_panel(
            lists,
            col=2,
            title="Topicos de resposta",
            placeholder="Topicos assinados para monitoramento leve",
            input_var=self.topic_input_var,
            add_command=lambda: self._add_to_list(self.topics_listbox, self.topic_input_var),
            remove_command=lambda: self._remove_selected(self.topics_listbox),
            up_command=lambda: self._move_selected(self.topics_listbox, -1),
            down_command=lambda: self._move_selected(self.topics_listbox, 1),
        )

        footer = tk.Frame(self.root, bg=BG, padx=16, pady=12)
        footer.pack(fill="x")

        self._button(footer, "Recarregar", self._load_data, outline=True).pack(side="left")
        self._button(footer, "Salvar configuracao", self._save_data).pack(side="left", padx=8)
        self._button(footer, "Iniciar Monitoramento", self._start_monitoring).pack(side="left", padx=8)
        self._button(footer, "Fechar", self.root.destroy, outline=True).pack(side="right")

        tk.Label(
            footer,
            textvariable=self.status_var,
            bg=BG,
            fg=TEXT,
            font=("Segoe UI", 9),
        ).pack(side="left", padx=20)

    def _build_settings(self, parent):
        parent.columnconfigure(1, weight=1)
        parent.columnconfigure(3, weight=1)

        self._field(parent, "Broker MQTT", self.broker_var, row=0, col=0)
        self._field(parent, "Porta", self.port_var, row=0, col=2)
        self._field(parent, "Topico resposta preciso", self.info_topic_var, row=1, col=0)
        self._field(parent, "Minimo de minutos", self.min_minutes_var, row=1, col=2)
        self._field(parent, "Maximo de minutos", self.max_minutes_var, row=2, col=0)
        self._field(parent, "Timeout resposta (s)", self.timeout_var, row=2, col=2)
        self._field(parent, "Intervalo ping (min)", self.ping_interval_var, row=3, col=0)
        self._field(parent, "Topico ping", self.ping_topic_var, row=3, col=2)
        self._field(parent, "Dashboard porta", self.dashboard_port_var, row=4, col=0)
        self._history_mode_field(parent, row=4, col=2)

    def _field(self, parent, label, variable, row, col):
        tk.Label(
            parent,
            text=label,
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 10, "bold"),
        ).grid(row=row, column=col, sticky="w", padx=(0, 8), pady=(8, 4))

        entry = tk.Entry(
            parent,
            textvariable=variable,
            bg=INPUT_BG,
            fg=TEXT,
            font=("Segoe UI", 10),
            relief="solid",
            bd=1,
            insertbackground=TEXT,
        )
        entry.grid(row=row, column=col + 1, sticky="ew", pady=(8, 4))

    def _history_mode_field(self, parent, row, col):
        tk.Label(
            parent,
            text="Historico do monitoramento",
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 10, "bold"),
        ).grid(row=row, column=col, sticky="w", padx=(0, 8), pady=(8, 4))

        choices = tk.Frame(parent, bg=PANEL)
        choices.grid(row=row, column=col + 1, sticky="w", pady=(8, 4))

        tk.Radiobutton(
            choices,
            text="Juntar com historico salvo",
            variable=self.history_mode_var,
            value="merge",
            bg=PANEL,
            fg=TEXT,
            activebackground=PANEL,
            activeforeground=TEXT,
            selectcolor=INPUT_BG,
            font=("Segoe UI", 10),
        ).pack(side="left", padx=(0, 18))

        tk.Radiobutton(
            choices,
            text="Comecar monitoramento novo (zero)",
            variable=self.history_mode_var,
            value="fresh",
            bg=PANEL,
            fg=TEXT,
            activebackground=PANEL,
            activeforeground=TEXT,
            selectcolor=INPUT_BG,
            font=("Segoe UI", 10),
        ).pack(side="left")

    def _build_list_panel(
        self,
        parent,
        col,
        title,
        placeholder,
        input_var,
        add_command,
        remove_command,
        up_command,
        down_command,
    ):
        panel = tk.Frame(parent, bg=PANEL, padx=12, pady=10, bd=1, relief="solid")
        panel.grid(row=0, column=col, sticky="nsew", padx=(0, 7) if col == 0 else (7, 0))
        panel.columnconfigure(0, weight=1)
        panel.rowconfigure(3, weight=1)

        tk.Label(
            panel,
            text=title,
            bg=PANEL,
            fg=TEXT,
            font=("Segoe UI", 12, "bold"),
        ).grid(row=0, column=0, sticky="w", pady=(0, 8))

        input_row = tk.Frame(panel, bg=PANEL)
        input_row.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        input_row.columnconfigure(0, weight=1)
        tk.Entry(
            input_row,
            textvariable=input_var,
            bg=INPUT_BG,
            fg=TEXT,
            font=("Segoe UI", 10),
            relief="solid",
            bd=1,
            insertbackground=TEXT,
        ).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        self._button(input_row, "Adicionar", add_command).grid(row=0, column=1)

        tk.Label(
            panel,
            text=placeholder,
            bg=PANEL,
            fg="#3C6E47",
            font=("Segoe UI", 9),
        ).grid(row=2, column=0, sticky="nw")

        list_row = tk.Frame(panel, bg=PANEL)
        list_row.grid(row=3, column=0, sticky="nsew")
        list_row.columnconfigure(0, weight=1)
        list_row.rowconfigure(0, weight=1)

        listbox = tk.Listbox(
            list_row,
            bg=INPUT_BG,
            fg=TEXT,
            font=("Consolas", 10),
            relief="solid",
            bd=1,
            selectbackground="#A5D6A7",
            selectforeground=TEXT,
        )
        listbox.grid(row=0, column=0, sticky="nsew")

        scrollbar = tk.Scrollbar(list_row, orient="vertical", command=listbox.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        listbox.configure(yscrollcommand=scrollbar.set)

        actions = tk.Frame(panel, bg=PANEL)
        actions.grid(row=4, column=0, sticky="ew", pady=(8, 0))
        self._button(actions, "Remover", remove_command, outline=True).pack(side="left")
        self._button(actions, "Subir", up_command, outline=True).pack(side="left", padx=6)
        self._button(actions, "Descer", down_command, outline=True).pack(side="left")

        return listbox

    def _button(self, parent, text, command, outline=False):
        if outline:
            return tk.Button(
                parent,
                text=text,
                command=command,
                bg=BG,
                fg=ACCENT_DARK,
                activebackground="#C8E6C9",
                activeforeground=ACCENT_DARK,
                relief="solid",
                bd=1,
                font=("Segoe UI", 9, "bold"),
                padx=10,
                pady=4,
                cursor="hand2",
            )
        return tk.Button(
            parent,
            text=text,
            command=command,
            bg=ACCENT,
            fg="white",
            activebackground=ACCENT_DARK,
            activeforeground="white",
            relief="flat",
            font=("Segoe UI", 9, "bold"),
            padx=12,
            pady=5,
            cursor="hand2",
        )

    def _clear_listbox(self, listbox):
        listbox.delete(0, tk.END)

    def _fill_listbox(self, listbox, values):
        self._clear_listbox(listbox)
        for value in values:
            listbox.insert(tk.END, value)

    def _listbox_values(self, listbox):
        return list(listbox.get(0, tk.END))

    def _add_to_list(self, listbox, input_var):
        value = input_var.get().strip()
        if not value:
            return
        current = self._listbox_values(listbox)
        if value in current:
            self.status_var.set(f"Valor ja existe: {value}")
            input_var.set("")
            return
        listbox.insert(tk.END, value)
        input_var.set("")
        self.status_var.set(f"Adicionado: {value}")

    def _remove_selected(self, listbox):
        selected = listbox.curselection()
        if not selected:
            return
        for index in reversed(selected):
            listbox.delete(index)
        self.status_var.set("Itens removidos.")

    def _move_selected(self, listbox, direction):
        selected = listbox.curselection()
        if not selected:
            return
        index = selected[0]
        target = index + direction
        if target < 0 or target >= listbox.size():
            return
        value = listbox.get(index)
        listbox.delete(index)
        listbox.insert(target, value)
        listbox.selection_clear(0, tk.END)
        listbox.selection_set(target)
        self.status_var.set("Ordem atualizada.")

    def _load_data(self):
        config = load_editable_config(self.config_path)

        self.broker_var.set(config["broker"])
        self.port_var.set(str(config["port"]))
        self.info_topic_var.set(config["info_topic"])
        self.min_minutes_var.set(str(config["min_minutes"]))
        self.max_minutes_var.set(str(config["max_minutes"]))
        self.timeout_var.set(str(config["response_timeout_sec"]))
        self.ping_interval_var.set(str(config["ping_interval_minutes"]))
        self.ping_topic_var.set(config["ping_topic"])
        self.dashboard_port_var.set(str(config["dashboard_port"]))
        self.history_mode_var.set(config["history_mode"])

        self._fill_listbox(self.cmd_topics_listbox, config["cmd_topics"])
        self._fill_listbox(self.topics_listbox, config["topics"])
        self._fill_listbox(self.filters_listbox, config["filter_names"])
        self.status_var.set(f"Configuracao carregada de {self.config_path}")

    def _save_data(self):
        normalized = self._persist_config(show_success=True)
        return normalized is not None

    def _persist_config(self, show_success):
        raw_config = {
            "broker": self.broker_var.get().strip(),
            "port": self.port_var.get().strip(),
            "info_topic": self.info_topic_var.get().strip(),
            "min_minutes": self.min_minutes_var.get().strip(),
            "max_minutes": self.max_minutes_var.get().strip(),
            "response_timeout_sec": self.timeout_var.get().strip(),
            "ping_interval_minutes": self.ping_interval_var.get().strip(),
            "ping_topic": self.ping_topic_var.get().strip(),
            "dashboard_port": self.dashboard_port_var.get().strip(),
            "history_mode": self.history_mode_var.get().strip(),
            "cmd_topics": self._listbox_values(self.cmd_topics_listbox),
            "topics": self._listbox_values(self.topics_listbox),
            "filter_names": self._listbox_values(self.filters_listbox),
        }

        normalized = normalize_config(raw_config)

        if not normalized["broker"]:
            messagebox.showerror("Erro", "Broker nao pode ficar vazio.")
            return None
        if not normalized["topics"]:
            messagebox.showerror("Erro", "Inclua pelo menos um topico.")
            return None
        if not normalized["cmd_topics"]:
            messagebox.showerror("Erro", "Inclua pelo menos um topico de comando (#11$).")
            return None
        if not normalized["ping_topic"]:
            messagebox.showerror("Erro", "Topico ping nao pode ficar vazio.")
            return None

        try:
            save_config(normalized, self.config_path)
        except OSError as exc:
            messagebox.showerror("Erro ao salvar", str(exc))
            return None

        # Reaplica valores normalizados na tela para manter estado consistente.
        self.broker_var.set(normalized["broker"])
        self.port_var.set(str(normalized["port"]))
        self.info_topic_var.set(normalized["info_topic"])
        self.min_minutes_var.set(str(normalized["min_minutes"]))
        self.max_minutes_var.set(str(normalized["max_minutes"]))
        self.timeout_var.set(str(normalized["response_timeout_sec"]))
        self.ping_interval_var.set(str(normalized["ping_interval_minutes"]))
        self.ping_topic_var.set(normalized["ping_topic"])
        self.dashboard_port_var.set(str(normalized["dashboard_port"]))
        self.history_mode_var.set(normalized["history_mode"])
        self._fill_listbox(self.cmd_topics_listbox, normalized["cmd_topics"])
        self._fill_listbox(self.topics_listbox, normalized["topics"])
        self._fill_listbox(self.filters_listbox, normalized["filter_names"])

        self.status_var.set(f"Configuracao salva em {self.config_path}")
        if show_success:
            messagebox.showinfo("Sucesso", "Configuracao salva com sucesso.")
        return normalized

    def _start_monitoring(self):
        normalized = self._persist_config(show_success=False)
        if not normalized:
            return

        base_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(base_dir, "cloudv2-ping-monitoring.py")
        if not os.path.exists(script_path):
            messagebox.showerror("Erro", f"Arquivo nao encontrado: {script_path}")
            return

        command = [sys.executable, script_path]
        creationflags = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)

        try:
            subprocess.Popen(
                command,
                cwd=base_dir,
                start_new_session=True,
                creationflags=creationflags,
            )
        except OSError as exc:
            messagebox.showerror("Erro ao iniciar", str(exc))
            return

        dashboard_url = f"http://localhost:{normalized['dashboard_port']}/index.html"
        self.status_var.set(f"Monitoramento iniciado. Abrindo {dashboard_url}")
        self.root.after(900, lambda: webbrowser.open_new_tab(dashboard_url))


def main():
    root = tk.Tk()
    ConfigUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
