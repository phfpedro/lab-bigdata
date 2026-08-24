# labs

Repositório de laboratório de códigos da Moinho Sul. Aqui ficam testes, experimentos e provas de conceito (PoCs).

## Propósito

Este repositório existe para que cada desenvolvedor possa **experimentar livremente**: testar ideias, validar abordagens técnicas e construir provas de conceito sem o compromisso de manter o código em produção.

> ⚠️ **Importante:** nada aqui é código de produção. Tudo tem validade apenas dentro do contexto do experimento para o qual foi criado.

## Como funciona

- Cada desenvolvedor cria uma **branch própria** para fazer seus testes ou provas de conceito.
- O código de uma branch tem validade **apenas para aquele contexto** — não é mantido, versionado oficialmente nem reaproveitado fora dele.
- **Branches de experimento nunca são mescladas na `main`.**

## A branch `main`

A `main` contém apenas o que é **padrão e comum** para iniciar testes e PoCs, por exemplo:

- Estruturas-base e scaffolding reutilizáveis
- Configurações compartilhadas
- Esta documentação

Ou seja: a `main` é o ponto de partida, não o destino dos experimentos.

## Fluxo de trabalho

1. Parta sempre da `main` atualizada:
   ```bash
   git checkout main
   git pull
   ```
2. Crie sua branch de experimento (sugestão de nomenclatura: `nome/descricao-do-teste`):
   ```bash
   git checkout -b marcelo/teste-cache-redis
   ```
3. Faça seus testes e PoCs à vontade nessa branch.
4. Ao concluir, a branch pode ser simplesmente **descartada**. Não abra Pull Request para a `main` com código experimental.

## Convenção de nomes de branch

```
<seu-nome>/<descricao-curta>
```

Exemplos:
- `marcelo/poc-fila-rabbitmq`
- `ana/teste-lib-grafico`
- `joao/spike-autenticacao-oauth`

## Boas práticas

- Use nomes de branch descritivos para que outros entendam o objetivo do experimento.
- Documente no próprio código (ou em um `NOTES.md` da branch) as conclusões do experimento, caso queira preservar o aprendizado.
- Lembre-se: o valor está no **aprendizado**, não no código. Sinta-se à vontade para errar e descartar.
