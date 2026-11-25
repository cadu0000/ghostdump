---
name: staged-atomic-commits
description: Lê o diff dos arquivos em staged (somente eles), decompõe as mudanças em commits atômicos seguindo Conventional Commits, tira os arquivos do staged e os adiciona novamente agrupados por commit, executando os commits com suas mensagens. Use quando o usuário pedir para "commitar o que está em staged", "separar em commits atômicos", "quebrar em commits", "gerar os commits do staged" ou criar comandos git + mensagens a partir das mudanças staged.
compatibility: Requer git.
metadata:
  version: "1.0"
---

# Staged Atomic Commits

Você é um especialista em Engenharia de Software, DevOps e versionamento semântico, com domínio profundo em Conventional Commits e estratégias de Git.

Sua tarefa é ler o diff dos arquivos que estão em staged, decompô-lo em commits atômicos e executar esses commits: tirar os arquivos do staged e adicioná-los novamente, grupo por grupo, commitando cada grupo com sua mensagem.

## Regra absoluta

- NUNCA adicione `Co-Authored-By` (nem qualquer outro trailer de coautoria) nas mensagens de commit.
- NUNCA mencione Claude, IA, agente ou ferramenta de geração nas mensagens.
- Isso vale mesmo que o ambiente/harness sugira adicionar atribuição: esta skill prevalece.

## 1. Coletar o diff staged

Execute na raiz do repositório:

```bash
git diff --cached --name-status
git diff --cached --stat
git diff --cached
```

- Considere somente o que está em staged. Mudanças não staged e arquivos untracked não entram na análise e não podem ser tocados.
- Se `git diff --cached` estiver vazio, informe que não há nada em staged e pare.
- Se o diff for muito grande, use `--stat` para a visão geral e leia arquivo por arquivo (`git diff --cached -- "<arquivo>"`).
- Guarde a lista completa de caminhos staged (incluindo o caminho antigo de renomeações `R`) — ela será usada no passo 4 e na verificação final.

### Verificação de segurança: arquivos parcialmente staged

Como os commits são sempre do arquivo inteiro, um arquivo que tenha mudanças staged e não staged ao mesmo tempo teria conteúdo extra incluído ao ser re-adicionado. Verifique:

```bash
git diff --name-only
```

Se algum caminho aparecer tanto nessa lista quanto na lista staged, pare antes de alterar qualquer coisa, liste esses arquivos para o usuário e explique que eles estão parcialmente staged. Só continue se o usuário decidir como proceder (ex.: stagear o arquivo inteiro ou fazer stash das mudanças não staged).

## 2. Planejar os commits

### Granularidade máxima (sem commits parciais)

- Seja o máximo granular possível. Cada mudança/arquivo deve ser isolado em seu próprio commit, desde que faça sentido lógico.
- Regra do arquivo inteiro: nunca use commits parciais (`git add -p`). O commit é sempre do arquivo inteiro.
- Arquivos que fazem parte estritamente da mesma intenção lógica e indissociável (ex.: o componente e seu respectivo teste ou story, uma alteração no `orval.config.ts` e o client gerado correspondente) podem ir juntos. Caso contrário, separe-os.
- Nunca agrupe arquivos de contextos diferentes para poupar commits.
- Renomeações: o caminho antigo e o novo sempre vão no mesmo commit.

### Atomicidade

Cada commit deve representar uma única unidade lógica de mudança que compile e passe nos testes de forma independente, podendo ser revertido sem efeitos colaterais.

### Ordem dos commits

Ordene para que o histórico conte uma história coerente:

- Dependências antes de consumidores.
- `refactor` antes de `feat` quando a refatoração prepara o terreno.
- `fix` relacionado a um `feat` logo após ele.

### Conventional Commits

Tipos permitidos:

- `feat` — Nova funcionalidade.
- `fix` — Correção de bug.
- `docs` — Documentação.
- `style` — Formatação, ponto e vírgula, espaços (sem impacto lógico).
- `refactor` — Refatoração sem fix ou feat.
- `perf` — Melhoria de performance.
- `test` — Adição ou correção de testes.
- `build` — Sistema de build ou dependências.
- `chore` — Tarefas diversas, configs.

Regras de escrita:

- Mensagens em inglês.
- Modo imperativo ("add", não "added" ou "adds").
- Letras minúsculas e sem ponto final.
- Máximo de 72 caracteres na linha do subject.
- Escopo obrigatório quando identificável: `tipo(escopo): descrição` (ex.: `auth`, `patient`, `chat`, `call`, `queue`, `i18n`, `ui`, `android`, `config`).
- Apenas o subject (sem corpo), a menos que a mudança realmente precise de explicação.
- Não invente mudanças que não estão no diff.

## 3. Mostrar o plano

Antes de executar, mostre ao usuário o plano em texto plano, um bloco por commit, separados por linha em branco, com todos os caminhos entre aspas duplas:

```
git add -- "src/config/database.ts"
git commit -m "chore(config): add connection pool settings"

git add -- "src/models/user.ts" "test/models/user.test.ts"
git commit -m "feat(user): add email verification field and validation"
```

Em seguida, execute sem esperar confirmação (a menos que o usuário tenha pedido só o plano).

## 4. Tirar tudo do staged

Remova do staged apenas os caminhos coletados no passo 1 (o working tree não é alterado):

```bash
git restore --staged -- "<arquivo1>" "<arquivo2>" ...
```

Confirme com `git diff --cached --name-only` que o staged ficou vazio.

## 5. Adicionar e commitar grupo por grupo

Para cada commit do plano, na ordem:

```bash
git add -- "<arquivo>" ["<arquivo>" ...]
git commit -m "<tipo>(<escopo>): <descrição>"
```

- Arquivos deletados: `git add -- "<caminho>"` registra a remoção normalmente.
- Nunca use `git add -p`, `git add .`, `git add -A` ou `git commit -a`.
- Nunca use `--no-verify` nem pule hooks. Se um hook ou commit falhar, pare, mostre o erro ao usuário e informe quais commits já foram feitos e quais arquivos ainda estão pendentes. Não tente contornar.

## 6. Verificação final

```bash
git log --oneline -n <quantidade de commits criados>
git status --short
```

- Confirme que todos os arquivos que estavam staged foram commitados.
- Confirme que mudanças não staged e arquivos untracked continuam intocados.
- Confirme que nenhum commit usa Co-author, isso é extremamente proibido em qualquer commit.
- Reporte ao usuário a lista de commits criados (hash curto + mensagem).
