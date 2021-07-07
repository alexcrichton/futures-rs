// Check that it works even if proc-macros are reexported.

fn main() {
    use futures04::{executor::block_on, future, stream, StreamExt};

    // join! macro
    let _ = block_on(async {
        let _ = futures04::join!(async {}, async {});
        let _ = macro_reexport::join!(async {}, async {});
        let _ = macro_reexport::join2!(async {}, async {});
    });

    // try_join! macro
    let _ = block_on(async {
        let _ = futures04::try_join!(async { Ok::<(), ()>(()) }, async { Ok::<(), ()>(()) });
        let _ = macro_reexport::try_join!(async { Ok::<(), ()>(()) }, async { Ok::<(), ()>(()) });
        let _ = macro_reexport::try_join2!(async { Ok::<(), ()>(()) }, async { Ok::<(), ()>(()) });
        Ok::<(), ()>(())
    });

    // select! macro
    let _ = block_on(async {
        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = futures04::select! {
            _ = a => {},
            _ = b => unreachable!(),
        };

        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = macro_reexport::select! {
            _ = a => {},
            _ = b => unreachable!(),
        };

        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = macro_reexport::select2! {
            _ = a => {},
            _ = b => unreachable!(),
        };
    });

    // select_biased! macro
    let _ = block_on(async {
        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = futures04::select_biased! {
            _ = a => {},
            _ = b => unreachable!(),
        };

        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = macro_reexport::select_biased! {
            _ = a => {},
            _ = b => unreachable!(),
        };

        let mut a = future::ready(());
        let mut b = future::pending::<()>();
        let _ = macro_reexport::select_biased2! {
            _ = a => {},
            _ = b => unreachable!(),
        };
    });

    // stream_select! macro
    let _ = block_on(async {
        let endless_ints = |i| stream::iter(vec![i].into_iter().cycle());

        let mut endless_ones = futures04::stream_select!(endless_ints(1i32), stream::pending());
        assert_eq!(endless_ones.next().await, Some(1));
        assert_eq!(endless_ones.next().await, Some(1));

        let mut finite_list = futures04::stream_select!(stream::iter(vec![1, 2, 3].into_iter()));
        assert_eq!(finite_list.next().await, Some(1));
        assert_eq!(finite_list.next().await, Some(2));
        assert_eq!(finite_list.next().await, Some(3));
        assert_eq!(finite_list.next().await, None);

        let endless_mixed =
            futures04::stream_select!(endless_ints(1i32), endless_ints(2), endless_ints(3));
        // Take 100, and assert a somewhat even distribution of values.
        // The fairness is randomized, but over 100 samples we should be pretty close to even.
        // This test may be a bit flaky. Feel free to adjust the margins as you see fit.
        let mut count = 0;
        let results = endless_mixed
            .take_while(move |_| {
                count += 1;
                let ret = count < 100;
                async move { ret }
            })
            .collect::<Vec<_>>()
            .await;
        assert!(results.iter().filter(|x| **x == 1).count() >= 29);
        assert!(results.iter().filter(|x| **x == 2).count() >= 29);
        assert!(results.iter().filter(|x| **x == 3).count() >= 29);
    });
}
