from sklearn.model_selection import RandomizedSearchCV
import logging

__all__ = ['ModelTester']


class ModelTester:
    def __init__(self, parameters, model, rnd_state, scoring='f1_macro', njobs=-1, cv=3):
        self.cv = RandomizedSearchCV(model, param_distributions=parameters, random_state=rnd_state, scoring=scoring, n_jobs=njobs, cv=cv, verbose=1)

    def test_model(self, x_train, y_train):
        logging.debug('Fitting cv')
        self.cv.fit(x_train, y_train)

        logging.info('Best score cv: {}'.format(self.cv.best_score_))

    def best_estimator(self):
        return self.cv.best_estimator_

    def print_importances(self, data_df):
        model = self.cv.best_estimator_

        if hasattr(model, 'feature_importances_'):
            self.__print_importances_internal(data_df, model.feature_importances_)
        elif hasattr(model, 'coef_'):
            self.__print_importances_internal(data_df, model.coef_.flatten())

    @staticmethod
    def __print_importances_internal(data_df, imp_list):
        print('Top 10 features:')
        val_zip = zip(data_df.columns, imp_list)
        for a, b, in sorted(val_zip, key=lambda zp_gb: zp_gb[1], reverse=True)[:10]:
            print("{0}: {1}".format(a, b))


