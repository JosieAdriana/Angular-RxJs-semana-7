/* eslint-disable */
import React, { useEffect, useRef } from 'react';
import RxViz from '../components/RxViz';
import Head from 'next/head';
import {
  // Classes base
  Observable, Subject,
  // operadores de criação de Observables
  fromEvent,
  interval,
  of,
  combineLatest,
  forkJoin,
  merge,
  concat,
  throwError,
} from 'rxjs';
import {
  // Operadores "pipeable", para serem utilizados dentro da função "pipe"
  map,
  mapTo,
  tap,
  mergeMap,
  mergeAll,
  switchMap,
  take,
  groupBy,
  delay,
  share,
  filter,
  debounceTime,
  throttleTime,
  retry,
  reduce,
  scan,
  withLatestFrom,
  switchMapTo,
  concatAll,
  startWith,
  distinctUntilChanged,
  exhaustMap,
  finalize,
  takeUntil,
  timeout,
  catchError,
  concatMap,
  bufferCount,
  skip
} from 'rxjs/operators';
import { any, number } from 'prop-types';
import { resourceLimits } from 'worker_threads';

let countClicks = 0;

// função para simular uma requisição com possibilidade de dar algum erro.
function simulateRequest(errorChance = 0.5, requestTime: number = 500, value = 'R') {
  console.log('request start')
  return of(value)
    .pipe(
      delay(requestTime),
      tap(() => {
        console.log('request end')
        if (Math.random() >= 1 - errorChance) {
          console.log('request error')
          throw new Error("Erro")
        }
      })
    )
}

// Função do componente do React
export default function Index() {

  const inputRef = useRef<HTMLInputElement>(null);

  // Abaixo estão alguns observables já configurados para podermos manipular com operadores.

  // Observable criado do zero com um producer
  const producer$ = new Observable<string>(subscriber => {
    // Essa arrow function é o nosso "Producer", pois ela vai emitir valores para cada subscriber por meio do next
    // por exemplo, emitindo o valor "P1" imediatamente, emitindo o valor "P2" após 2 segundos e completando.
    subscriber.next("P1")
    setTimeout(() => {
      // subscriber.next("P2")
      subscriber.error()
    }, 2000)
  })
  // Esse observable emite um valor incremental a cada segundo, começando em 0, 
  // criado a partir do operador estático "interval"
  const count$ = interval(1000);
  // Emite "C" sempre que a tela é clicada
  const click$ = new Subject<string>();
  // Emite uma tupla com as coordenadas [x, y] do mouse sempre que ele se mover
  const mouseMove$ = new Subject<[x: number, y: number]>();
  // Emite "MD" quando o mouse é pressionado
  const mouseDown$ = new Subject<"MD">()
  // Emite "MU" quando o mouse é levantado
  const mouseUp$ = new Subject<"MU">()
  // Emite o caractere digitado no campo de input
  const key$ = new Subject<string>()
  // Emite todo o texto do campo de input sempre que ele muda
  const input$ = new Subject<string>()


  useEffect(() => {
    // Abaixo adicionamos os listeners para emitir os eventos respectivos nos Subjects por meio do next()
    // Não é necessário alterar nada aqui nessa parte
    document.addEventListener('click', (e) => {
      countClicks++;
      click$.next(`C${countClicks}`)
    })
    document.addEventListener('mousemove', (e) => {
      mouseMove$.next([e.clientX, e.clientY])
    })
    document.addEventListener('mousedown', (e) => {
      mouseDown$.next('MD')
    })
    document.addEventListener('mouseup', (e) => {
      mouseUp$.next('MU')
    })
    inputRef.current?.addEventListener('keydown', (e) => {
      key$.next(e.key)
    })
    inputRef.current?.addEventListener('input', (e) => {
      const target = e.target as HTMLInputElement;
      input$.next(target.value)
    })
  })

  // De forma geral, os Observables "cold" não fazem nada sem terem um subscriber, 
  // feito ao chamar o método ".subscribe(observer)" e passando um objeto de Observer
  // com os callbacks de next/error/complete:
  const subscription = count$.subscribe({
    next: (value) => {
      console.log(`next count$: ${value}`)
    },
    error: (error) => {
      console.log(error)
    },
    complete: () => {
      console.log('complete')
    },
  })
  // podemos cancelar a subscription pelo método unsubscribe(),
  // porém, observables que completam ou tem erro já encerram automaticamente
  // e não é necessário fazer o unsubscribe manualmente (ex: chamadas http no Angular)
  subscription.unsubscribe()

  // Representa o tempo máximo representado na tela, ajuste para mais ou menos se preferir. Padrão de 30 segundos.
  const TIME = 30000

  //************************************************/
  //  SETAR ESSAS TRÊS CONSTANTES PARA REALIZAR OS TESTES
  const input1$ = count$
    .pipe(
      share(),
    );
  const input2$ = mouseMove$
    .pipe(
      startWith("CI")
    );
  const output$ = count$
    .pipe(
      skip(3)
    );
  //Exercícios:
  // 1 Faça um observable que transforme os valores de interval$ em um valor constante (mapTo)

  const e1$ = count$
    .pipe(
      mapTo(5),
    );

  // 2. Faça um observable que transforme os valores de interval$ em um valor calculado (map)
  
  const e2$ = count$
    .pipe(
      map(x => x * 10),
    );

  // 3. Faça um observable que emita a soma dos valores emitidos pelo interval$ toda vez que ele emitir um valor (scan)

  const e3$ = count$
    .pipe(
      scan((acc, v) => {
        return acc + v
      })
    );

  // 4. Faça um observable que sempre que se fazer um click$, ele dispare uma requisição (simulateRequest) e emita o resultado no mesmo observable (mergeMap)

  const e4$ = click$
    .pipe(
      mergeMap((x, i) => simulateRequest(0, 1000 * i, `R${x}`))
    );

  // 5. Faça um observable que sempre que se um fazer click$, ele  dispare uma requisição (simulateRequest) e emita o resultado no mesmo observable, mas se for clickado uma outra vez antes da requisição terminar, ele cancele a requisição anterior e passe a escutar somente o resultado da requisição mais recente (switchMap)

  const e5$ = click$
    .pipe(
      switchMap((x, i) => simulateRequest(0, 500 * i, `R${x}`))

    );

  // 6. Faça um observable que sempre que se um fazer click$, ele  dispare uma requisição (simulateRequest) e emita o resultado no mesmo observable, mas se for clickado mais vezes antes da requisição terminar, ele ignore os cliques até que a requisição seja terminada (exhaustMap)

  const e6$ = click$
    .pipe(
      exhaustMap((x, i) => simulateRequest(0, 1000 * i, `R${x}`))
    );

  // 7. Faça um observable que emita somente quando o usuário pressionar a key$ de "enter". (filter)

  const e7$ = key$
    .pipe(
      filter(input => input.trim() === "Enter")
    );

  // 8. Faça um observable que emita o valor total do input de texto somente quando o usuário parar de digitar por mais de 300 milisegundos (throttleTime)

  const e8$ = input$
    .pipe(
      throttleTime(30000)
    );

  // 9. Usando o observable do exercício 8, Simule uma situação de "pesquisa", ou seja, crie um novo observable que dispara uma requisição ao receber esse valor do input, e considera somente a última requisição caso seja emitido outro valor de input. (switchMap)

  const e9$ = input$
    .pipe(
      throttleTime(300),
      switchMap((x, i) => simulateRequest(0, 400 * i, `R${x}`))
    );

  // 10. Faça com que o observable de key$ não emita valores repetidos em sequência (distinctUntilChanged)

  const e10$ = key$
    .pipe(
      distinctUntilChanged(),
      map((x, i) => simulateRequest(0, 300))
    );

  // 11. Faça um observable que combine os últimos valores emitidos pelo interval$, click$ e input$ e emita sua combinação como uma tupla (combineLatest)

  const e11$ = combineLatest(
    [click$, input$]
  );

  // 12. Faça um observable que periodicamente (interval$) emita o último valor digitado no input (withLatestFrom + map)

  const e12$ = count$
    .pipe(
      withLatestFrom(input$),
      map(x => x)
    );

  //  13. Faça um observable que comece a emitir os valores de mouseMove$ somente quando ele estiver com o mouse pressionado (mouseDown$) e para de emitir quando o mouse levantar (mouseUp$)

  const e13$ = mouseMove$
    .pipe(
      withLatestFrom(mouseDown$),
      takeUntil(mouseUp$)
    );

  // Array de observables que será renderizado na tela, já vai ser feita a subscription em cada um deles pelo componente de renderização.
  // Comente e descomente as linhas para facilitar a sua visualização
  const observables: Array<[name: string, observable: Observable<any>]> = [
    // ["count$", count$],
    // ["producer$", producer$],
    // ["mouseMove$", mouseMove$],
    // ["mouseDown$", mouseDown$],
    // ["mouseUp$", mouseUp$],
    // ["click$", click$],
    // ["key$", key$],
    // ["input$", input$],
    // ["input1$", input1$],
    // ["input2$", input2$],
    // ["output$", output$],
    ["e1", e1$],
    ["e2", e2$],
    ["e3", e3$],
    ["e4", e4$],
    ["e5", e5$],
    ["e6", e6$],
    ["e7", e7$],
    ["e8", e8$],
    ["e9", e9$],
    ["e10", e10$],
    ["e11", e11$],
    ["e12", e12$],
    ["e13", e13$]
  ]
  /************************************************/

  return (

    <div className="container" style={{ margin: "15px" }}>
      <Head>
        <title>RxViz - Devschool</title>
        <meta charSet="utf-8" />
        <meta
          name="viewport"
          content="initial-scale=1.0, width=device-width"
        />
        <link
          href="https://fonts.googleapis.com/css?family=Roboto|Roboto+Mono|Montserrat:700"
          rel="stylesheet"
        />
        <style>{`body { margin: 0; font-family: Roboto }`}</style>
      </Head>
      <span>Input de texto: </span><input id='input' ref={inputRef} />
      {observables.map(([name, obs$]) => <div key={name}>
        <div><strong>{name}</strong></div>
        <RxViz
          timeWindow={TIME}
          observable$={obs$}
        />
      </div>
      )}
    </div>
  )
}


