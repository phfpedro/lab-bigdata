# Como contribuir

Este repositório é um **laboratório de experimentos e provas de conceito (PoCs)**. As regras abaixo existem para manter a `main` limpa e o histórico organizado, sem engessar a liberdade de experimentar.

## O que pode e o que não pode ir para a `main`

**Pode ir para a `main`:**

- Documentação (`README.md`, este `CONTRIBUTING.md`, etc.)
- Estruturas-base e scaffolding reutilizáveis (templates de PoC)
- Configurações compartilhadas (linters, `.gitignore`, `docker-compose` de serviços comuns)
- Snippets e utilitários genéricos que servem para vários experimentos

**Não pode ir para a `main`:**

- Código experimental, testes pontuais ou provas de conceito
- Qualquer coisa com validade restrita ao contexto de um único experimento
- Credenciais, segredos ou dados sensíveis (em branch nenhuma)

> Regra de ouro: se o código só faz sentido dentro do seu experimento, ele fica na **sua branch** — nunca na `main`.

## Nomenclatura de branch

Use o padrão:

```
<seu-nome>/<descricao-curta>
```

A descrição deve ser curta, em minúsculas e separada por hífens. Quando ajudar, prefixe com o tipo do trabalho (`poc`, `teste`, `spike`).

Exemplos:

- `marcelo/poc-fila-rabbitmq`
- `ana/teste-lib-grafico`
- `joao/spike-autenticacao-oauth`

## Fluxo de trabalho

1. Parta sempre da `main` atualizada:
   ```bash
   git checkout main
   git pull
   ```
2. Crie sua branch de experimento:
   ```bash
   git checkout -b seu-nome/descricao-do-teste
   ```
3. Experimente à vontade. Se quiser preservar o aprendizado, registre as conclusões no código ou em um `NOTES.md` da branch.
4. Ao concluir, **descarte a branch**. Não abra Pull Request para a `main` com código experimental.

## Mudanças na `main`

Alterações na `main` (templates, configs, docs) devem ser feitas por **Pull Request** e revisadas por pelo menos uma pessoa, garantindo que apenas conteúdo padrão e reutilizável entre.

## Política de descarte e limpeza de branches

- Branches de experimento são **temporárias** e devem ser apagadas após o término do PoC.
- Apague sua própria branch quando concluir:
  ```bash
  git branch -d seu-nome/descricao-do-teste          # local
  git push origin --delete seu-nome/descricao-do-teste  # remota
  ```
- Branches sem atividade por **90 dias** podem ser removidas por qualquer mantenedor, sem aviso individual.
- O aprendizado de um experimento deve ser preservado fora do código (documento interno, wiki, anotações) — a branch em si é descartável.
