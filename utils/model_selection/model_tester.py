from sklearn.model_selection import GridSearchCV
import logging

__all__ = ['ModelTester']


class ModelTester:
    def __init__(self, parameters, model, scoring='f1_macro', njobs=-1, cv=3):
        self.cv = GridSearchCV(model, param_grid=parameters, scoring=scoring, n_jobs=njobs, cv=cv, verbose=1)

    def test_model(self, Xtrain, ytrain):
        logging.debug('Fitting cv')
        self.cv.fit(Xtrain, ytrain);

        logging.info('Best score cv: ', self.cv.best_score_)
        logging.info('Params: ', self.cv.best_params_)

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


